import time

from concurrent.futures.thread import ThreadPoolExecutor
from threading import Event
from typing import List, Set

import arrow

from cognite.client.data_classes import TimeSeries
from cognite.extractorutils.statestore import AbstractStateStore
from cognite.extractorutils.throttle import throttled_loop
from cognite.extractorutils.uploader import TimeSeriesUploadQueue

from .config import IceCreamFactoryConfig
from .ice_cream_factory_api import IceCreamFactoryAPI


class Backfiller:
    """
    Query the Ice Cream Factory API for historical data for the given time frame and uploads datapoints to CDF clean.
    Backfills from utc "now" back to limit set by config/data input in Cognite functions.

    Args:
        upload_queue: Where to put data points
        stop: Stopping event
        api: API to query
        time_series: List of timeseries to query datapoints for and back fill
        config: Set of configuration parameters
        states: Current state of time series in CDF
    """

    def __init__(
        self,
        upload_queue: TimeSeriesUploadQueue,
        stop: Event,
        api: IceCreamFactoryAPI,
        timeseries_list: List[TimeSeries],
        config: IceCreamFactoryConfig,
        states: AbstractStateStore,
    ):
        # Target iteration time to allow some throttling between iterations
        self.target_iteration_time = 2 * len(timeseries_list)
        self.upload_queue = upload_queue
        self.stop = stop
        self.api = api
        self.config = config

        # Create a copy of list, so we can delete from it when the backfill for a timeseries is done
        self.timeseries_list = timeseries_list.copy()
        self.states = states
        self.stop_at = arrow.utcnow().shift(minutes=-config.backfill.backfill_min)
        self.now_ts = arrow.utcnow().float_timestamp * 1000
        self.timeseries_seen_set: Set[str] = set()

    def _extract_time_series(self, timeseries: TimeSeries) -> None:
        """
        Perform a query for a given time series. Function to send to thread pool in run().

        Args:
            timeseries: timeseries to get datapoints for
        """
        timestamps: List[float] = []
        states = self.states.get_state(timeseries.external_id)

        first_datapoint = states[0]
        if first_datapoint is not None and timeseries.external_id in self.timeseries_seen_set:
            timestamps.append(first_datapoint)

        if len(timestamps) == 0:
            # No previous data for timeseries, or start of backfilling loop. Backfill from now
            timestamps.append(self.now_ts)
            self.timeseries_seen_set.add(timeseries.external_id)

        to_time = arrow.get((max(timestamps) / 1000))
        from_time = to_time.shift(minutes=-10)  # can query API for only 10 min of data

        if from_time < self.stop_at:
            print(f"{timeseries.external_id} reached configured limit at {self.stop_at}")
            from_time = self.stop_at
            self.timeseries_list.remove(timeseries)

        print(f"Getting data for {timeseries.external_id} " f"from {from_time.isoformat()} to {to_time.isoformat()}")

        datapoints_dict = self.api.get_oee_timeseries_datapoints(
            timeseries_ext_id=timeseries.external_id, start=from_time.timestamp(), end=to_time.timestamp()
        )

        for timeseries_ext_id in datapoints_dict:
            # API returns 2 associated timeseries.
            self.upload_queue.add_to_upload_queue(
                external_id=timeseries_ext_id, datapoints=datapoints_dict[timeseries_ext_id]
            )

    def run(self) -> None:
        """
        Run backfiller until the low watermark has reached the configured backfill-min limit, or until the stop event is
        set.
        """
        with ThreadPoolExecutor(
            max_workers=self.config.extractor.parallelism, thread_name_prefix="Backfiller"
        ) as executor:
            for _ in throttled_loop(self.target_iteration_time, self.stop):
                futures = []

                # Make copy of list for this iteration to make list deletion in _extract_time_series safe
                timeseries_list = self.timeseries_list.copy()

                for ts in timeseries_list:
                    futures.append(executor.submit(self._extract_time_series, ts))
                    time.sleep(1.5)  # to not overload api

                for future in futures:
                    # result() is blocking until task is complete
                    future.result()

                if len(self.timeseries_list) == 0:
                    # All backfilling reached the end
                    print("Backfilling done")

                    # wait to ensure datapoints are uploaded and states are stored in CDF Raw
                    time.sleep(20)
                    self.stop.set()
                    return
