import logging
from concurrent.futures.thread import ThreadPoolExecutor
from threading import Event
from typing import List

import arrow
from cognite.extractorutils.statestore import AbstractStateStore
from cognite.extractorutils.throttle import throttled_loop
from cognite.extractorutils.uploader import TimeSeriesUploadQueue

from .config import WeatherConfig
from .met_client import FrostApi, WeatherStation

_logger = logging.getLogger(__name__)


def create_external_id(external_id_prefix, weather_station: WeatherStation, element: str) -> str:
    """
    Create the external ID of a time series.

    Args:
        external_id_prefix: Configured prefix for all external ID from this deployment
        weather_station: Weather station the time series is associated with
        element: Specific measurement element (air_temperature, wind_speed, etc)

    Returns:
        A generated external ID
    """
    return f"{external_id_prefix}{weather_station.id}_{element}"


def frontfill(
    upload_queue: TimeSeriesUploadQueue,
    frost: FrostApi,
    weather_stations: List[WeatherStation],
    config: WeatherConfig,
    states: AbstractStateStore,
) -> None:
    """
    Query the Frost API for all the data points missing since last run ended to ensure completeness in CDF.

    Args:
        upload_queue: Where to put data points
        frost: Frost API to query
        weather_stations: List of weather stations to frontfill data for
        config: Set of configuration parameters
        states: Current state of time series in CDF
    """

    def perform_frontfill(weather_station: WeatherStation) -> None:
        """
        Perform a query for a given weather station. Function to send to thread pool below.

        Args:
            weather_station: Station to get data for
        """
        timestamps: List[float] = []
        for element in config.frost.elements:
            ts = states.get_state(create_external_id(config.cognite.external_id_prefix, weather_station, element))[1]
            if ts is not None:
                # High watermark exist -> time series has previous data, so frontfill it
                timestamps.append(ts)

        if len(timestamps) == 0:
            # No previous data for weather station, skipping
            _logger.info(f"Skipping {weather_station.name}")
            return

        from_time, to_time = arrow.get(min(timestamps) / 1000), arrow.now()
        _logger.info(f"Getting data for {weather_station.name} from {from_time.isoformat()} to {to_time.isoformat()}")
        data = frost.get_historical(weather_station, config.frost.elements, from_time, to_time)

        for element in data:
            upload_queue.add_to_upload_queue(
                external_id=create_external_id(config.cognite.external_id_prefix, weather_station, element),
                datapoints=data[element],
            )

    # Run perform_frontfill on all weather stations
    with ThreadPoolExecutor(max_workers=config.extractor.parallelism, thread_name_prefix="Frontfiller") as executor:
        for weather_station in weather_stations:
            executor.submit(perform_frontfill, weather_station)

    _logger.info("Frontfilling done")


class Streamer:
    """
    Periodically query the Frost API for the current state of all the configured elements.

    Args:
        upload_queue: Where to put data points
        stop: Stopping event
        frost: Frost API to query
        weather_stations: List of weather stations to frontfill data for
        config: Set of configuration parameters
    """

    # 1 min total iteration time (usual update frequency is 10 mins in the Frost API)
    target_iteration_time = 60

    def __init__(
        self,
        upload_queue: TimeSeriesUploadQueue,
        stop: Event,
        frost: FrostApi,
        weather_stations: List[WeatherStation],
        config: WeatherConfig,
    ):
        self.upload_queue = upload_queue
        self.stop = stop
        self.frost = frost

        self.config = config

        self.weather_stations = weather_stations

    def _extract_weather_station(self, weather_station: WeatherStation) -> None:
        """
        Perform a query for a given weather station. Function to send to thread pool in run().

        Args:
            weather_station: Station to get data for
        """
        _logger.info(f"Getting live data for {weather_station.name}")

        data = self.frost.get_current(weather_station, self.config.frost.elements)

        for element in data:
            self.upload_queue.add_to_upload_queue(
                external_id=create_external_id(self.config.cognite.external_id_prefix, weather_station, element),
                datapoints=[data[element]],
            )

    def run(self) -> None:
        """
        Run streamer until the stop event is set.
        """
        with ThreadPoolExecutor(
            max_workers=self.config.extractor.parallelism, thread_name_prefix="Streamer"
        ) as executor:
            for _ in throttled_loop(Streamer.target_iteration_time, self.stop):
                futures = []

                for weather_station in self.weather_stations:
                    futures.append(executor.submit(self._extract_weather_station, weather_station))

                for future in futures:
                    # result() is blocking until task is complete
                    future.result()


class Backfiller:
    """
    Periodically query the Frost API for a day of historical data for all the configured elements.

    Args:
        upload_queue: Where to put data points
        stop: Stopping event
        frost: Frost API to query
        weather_stations: List of weather stations to frontfill data for
        config: Set of configuration parameters
        states: Current state of time series in CDF
    """

    # Target iteration time 5 secs to allow some throttling between iterations
    target_iteration_time = 5

    def __init__(
        self,
        upload_queue: TimeSeriesUploadQueue,
        stop: Event,
        frost: FrostApi,
        weather_stations: List[WeatherStation],
        config: WeatherConfig,
        states: AbstractStateStore,
    ):
        self.upload_queue = upload_queue
        self.stop = stop
        self.frost = frost

        self.config = config

        # Create a copy of list, so we can delete from it when the backfill for a station is done without messing up the
        # streamer
        self.weather_stations = weather_stations.copy()

        self.states = states

        self.stop_at = arrow.get(config.backfill.backfill_to)

    def _extract_weather_station(self, weather_station: WeatherStation) -> None:
        """
        Perform a query for a given weather station. Function to send to thread pool in run().

        Args:
            weather_station: Station to get data for
        """
        timestamps: List[float] = []
        for element in self.config.frost.elements:
            ts = self.states.get_state(
                create_external_id(self.config.cognite.external_id_prefix, weather_station, element)
            )[0]
            if ts is not None:
                timestamps.append(ts)

        if len(timestamps) == 0:
            # No previous data for weather station, backfill from now
            timestamps.append(arrow.utcnow().float_timestamp * 1000)

        to_time = arrow.get(max(timestamps) / 1000)
        from_time = to_time.shift(days=-7)

        if from_time < self.stop_at:
            _logger.info(f"{weather_station.name} reached configured limit at {self.stop_at}")
            from_time = self.stop_at
            self.weather_stations.remove(weather_station)

        _logger.info(f"Getting data for {weather_station.name} from {from_time.isoformat()} to {to_time.isoformat()}")
        data = self.frost.get_historical(weather_station, self.config.frost.elements, from_time, to_time)

        for element in data:
            self.upload_queue.add_to_upload_queue(
                external_id=create_external_id(self.config.cognite.external_id_prefix, weather_station, element),
                datapoints=data[element],
            )

    def run(self) -> None:
        """
        Run backfiller until the low watermark has reached the configured backfill-to limit, or until the stop event is
        set.
        """
        with ThreadPoolExecutor(
            max_workers=self.config.extractor.parallelism, thread_name_prefix="Backfiller"
        ) as executor:
            for _ in throttled_loop(Backfiller.target_iteration_time, self.stop):
                futures = []

                # Make copy of list for this iteration to make list deletion in _extract_weather_station safe
                weather_stations = self.weather_stations.copy()

                for weather_station in weather_stations:
                    futures.append(executor.submit(self._extract_weather_station, weather_station))

                for future in futures:
                    # result() is blocking until task is complete
                    future.result()

                if len(self.weather_stations) == 0:
                    # All backfilling reached the end
                    _logger.info("Backfilling done")
                    return
