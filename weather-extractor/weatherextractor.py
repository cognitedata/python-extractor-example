import logging
from concurrent.futures.thread import ThreadPoolExecutor
from threading import Event
from time import time
from typing import List

from cognite.extractorutils.uploader import TimeSeriesUploadQueue

from met_client import FrostApi, WeatherStation
from weatherconfig import WeatherConfig


def create_external_id(external_id_prefix, weather_station: WeatherStation, element: str):
    return f"{external_id_prefix}{weather_station.id}_{element}"


class Streamer:
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

        self.logger = logging.getLogger(__name__)

    def _extract_weather_station(self, weather_station: WeatherStation):
        self.logger.info(f"Getting live data for {weather_station.name}")

        data = self.frost.get_current(weather_station, self.config.frost.elements)

        for element in data:
            self.upload_queue.add_to_upload_queue(
                external_id=create_external_id(self.config.cognite.external_id_prefix, weather_station, element),
                datapoints=[data[element]],
            )

    def run(self):
        with ThreadPoolExecutor(
            max_workers=self.config.extractor.parallelism, thread_name_prefix="FrontFiller"
        ) as executor:
            while not self.stop.is_set():
                start_time = time()

                futures = []

                for weather_station in self.weather_stations:
                    futures.append(executor.submit(self._extract_weather_station, weather_station))

                for future in futures:
                    # result() is blocking until task is complete
                    future.result()

                # Throttle
                iteration_time = time() - start_time
                wait_time = Streamer.target_iteration_time - iteration_time
                self.logger.info(
                    f"Iteration done in {iteration_time:.1f} s, waiting {wait_time:.1f} s before next query"
                )
                if wait_time > 0:
                    self.stop.wait(wait_time)
