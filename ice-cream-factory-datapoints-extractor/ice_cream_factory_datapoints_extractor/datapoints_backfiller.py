import logging
from threading import Event
from typing import List, Set

import arrow
from cognite.client.data_classes import TimeSeries
from cognite.extractorutils.statestore import AbstractStateStore
from cognite.extractorutils.uploader import TimeSeriesUploadQueue
from retry import retry

from ice_cream_factory_datapoints_extractor.config import IceCreamFactoryConfig
from ice_cream_factory_datapoints_extractor.ice_cream_factory_api import IceCreamFactoryAPI


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
        self.logger = logging.getLogger(__name__)
        self.timeseries_list = timeseries_list
        self.states = states
        self.stop_at = arrow.utcnow().shift(days=-config.backfill.history_days)
        self.now_ts = arrow.utcnow()
        self.timeseries_seen_set: Set[str] = set()

    @retry(tries=10)
    def _extract_time_series(self, timeseries: TimeSeries) -> None:
        """
        Perform a query for a given time series. Function to send to thread pool in run().

        Args:
            timeseries: timeseries to get datapoints for
        """
        low, high = self.states.get_state(timeseries.external_id)
        if not low:
            low = self.now_ts.float_timestamp
        if not high:
            high = self.now_ts.float_timestamp

        earliest_start = min(low, self.stop_at.float_timestamp)
        latest_start = max(low, self.stop_at.float_timestamp)
        earliest_end = min(high, self.now_ts.float_timestamp)
        latest_end = max(high, self.now_ts.float_timestamp)

        self.process(timeseries, arrow.get(earliest_start), arrow.get(latest_start))
        self.process(timeseries, arrow.get(earliest_end), arrow.get(latest_end))
        logging.info(f"{timeseries.external_id} reached configured limit at {arrow.get(latest_end)}")

    def process(self, timeseries, start, end):
        logging.info(f"Getting historical data {timeseries.external_id} from {start} to {end}")
        single_query_lookback = - min(2, self.config.backfill.history_days)
        while end > start and not self.stop.is_set():

            from_time = end.shift(days=single_query_lookback)  # can query API for only 10 min of data

            logging.info(f"\t{timeseries.external_id} from {from_time.isoformat()} to {end.isoformat()}")

            datapoints_dict = self.api.get_oee_timeseries_datapoints(
                timeseries_ext_id=timeseries.external_id, start=from_time.timestamp(), end=end.timestamp()
            )

            for timeseries_ext_id in datapoints_dict:
                # API returns 2 associated timeseries.
                self.upload_queue.add_to_upload_queue(
                    external_id=timeseries_ext_id, datapoints=datapoints_dict[timeseries_ext_id]
                )

            end = from_time

    def run(self) -> None:
        """
        Run backfiller until the low watermark has reached the configured backfill-min limit, or until the stop event is
        set.
        """
        for ts in self.timeseries_list:
            self._extract_time_series(ts)
