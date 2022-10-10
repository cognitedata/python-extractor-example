from concurrent.futures.thread import ThreadPoolExecutor
from threading import Event
from typing import List, Set

import arrow

from cognite.client.data_classes import TimeSeries
from cognite.extractorutils.throttle import throttled_loop
from cognite.extractorutils.uploader import TimeSeriesUploadQueue

from .config import IceCreamFactoryConfig
from .ice_cream_factory_api import IceCreamFactoryAPI
import time

class Streamer:
    """
    Periodically query the Ice Cream Factory API for datapoints.

    Args:
        upload_queue: Where to put data points
        stop: Stopping event
        api: API to query
        timeseries_list: List of timeseries to query datapoints for
        config: Set of configuration parameters
    """

    def __init__(
            self,
            upload_queue: TimeSeriesUploadQueue,
            stop: Event,
            api: IceCreamFactoryAPI,
            timeseries_list: List[TimeSeries],
            config: IceCreamFactoryConfig,
    ):
        # Target iteration time to allow some throttling between iterations
        self.target_iteration_time = int(1.5 * len(timeseries_list))
        self.upload_queue = upload_queue
        self.stop = stop
        self.api = api
        self.config = config

        self.timeseries_list = timeseries_list
        self.timeseries_seen_set: Set[str] = set()

    def _extract_timeseries(self, timeseries: TimeSeries) -> None:
        """
        Perform a query for a given time series. Function to send to thread pool in run().

        Args:
            timeseries: timeseries to get datapoints for
        """
        print(f"Getting live data for {timeseries.external_id}")
        to_time = arrow.utcnow()
        from_time = to_time.shift(minutes=-10)

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
        Run streamer until the stop event is set.
        """
        with ThreadPoolExecutor(
            max_workers=self.config.extractor.parallelism, thread_name_prefix="Streamer"
        ) as executor:
            for _ in throttled_loop(self.target_iteration_time, self.stop):
                futures = []

                for timeseries in self.timeseries_list:
                    futures.append(executor.submit(self._extract_timeseries, timeseries))
                    time.sleep(1.2)  # to not overload api

                for future in futures:
                    # result() is blocking until task is complete
                    future.result()