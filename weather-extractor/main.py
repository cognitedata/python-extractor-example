import logging
import signal
import sys
from threading import Event, Thread
from typing import Dict, List, Optional

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, AssetList, TimeSeries
from cognite.extractorutils.configtools import load_yaml
from cognite.extractorutils.uploader import TimeSeriesUploadQueue
from cognite.extractorutils.util import ensure_time_series

from met_client import FrostApi, WeatherStation
from weatherconfig import LocationConfig, WeatherConfig
from weatherextractor import Streamer, create_external_id, frontfill


def init_stations(locations: List[LocationConfig], frost: FrostApi) -> List[WeatherStation]:
    weather_stations: List[WeatherStation] = []

    for location in locations:
        if location.station_id is not None:
            weather_stations.append(frost.get_station(location.station_id))
        else:
            weather_stations.append(frost.get_closest_station(longitude=location.longitude, latitude=location.latitude))

    return weather_stations


def list_time_series(
    weather_stations: List[WeatherStation], config: WeatherConfig, assets: Optional[Dict[WeatherStation, int]]
) -> List[TimeSeries]:
    time_series = []

    for weather_station in weather_stations:
        for element in config.frost.elements:
            external_id = create_external_id(config.cognite.external_id_prefix, weather_station, element)

            args = {
                "external_id": external_id,
                "legacy_name": external_id,
                "name": f"{weather_station.name}: {element.replace('_', ' ')}",
            }

            if config.extractor.create_assets:
                args["asset_id"] = assets[weather_station]

            time_series.append(TimeSeries(**args))

    return time_series


def create_assets(
    weather_stations: List[WeatherStation], config: WeatherConfig, cdf: CogniteClient
) -> Dict[WeatherStation, int]:
    assets = []

    for weather_station in weather_stations:
        assets.append(
            Asset(
                external_id=f"{config.cognite.external_id_prefix}{weather_station.id}",
                name=weather_station.name,
                source="Frost",
                metadata={
                    "longitude": str(weather_station.longitude),
                    "latitude": str(weather_station.latitude),
                    "station_id": weather_station.id,
                },
            )
        )

    # Todo: handle if (some) assets exists
    created_assets = cdf.assets.create(assets)

    station_to_asset_id = {}

    for asset in created_assets:
        weather_station = [s for s in weather_stations if s.id == asset.metadata["station_id"]][0]
        station_to_asset_id[weather_station] = asset.id

    return station_to_asset_id


if __name__ == "__main__":
    with open(sys.argv[1]) as config_file:
        config: WeatherConfig = load_yaml(config_file, WeatherConfig)

    config.logger.setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("Starting example Frost extractor")

    frost = FrostApi(config.frost.client_id)
    cdf = config.cognite.get_cognite_client("weather-extractor")
    state_store = config.extractor.state_store.create_state_store(cdf)
    state_store.initialize()

    logger.info("Getting info about weather stations")
    weather_stations = init_stations(config.locations, frost)

    if config.extractor.create_assets:
        assets = create_assets(weather_stations, config, cdf)
    else:
        assets = None

    time_series = list_time_series(weather_stations, config, assets)

    logger.info(f"Ensuring that {len(time_series)} time series exist in CDF")
    ensure_time_series(cdf, time_series)

    # Create a stopping condition
    stop = Event()

    # Reroute ctrl-C to trigger the stopping condition instead of exiting uncleanly
    def sigint_handler(sig, frame):
        print()  # ensure newline before log
        logger.warning("Interrupt signal received, stopping")
        stop.set()
        logger.info("Waiting for threads to complete")

    signal.signal(signal.SIGINT, sigint_handler)

    if config.metrics:
        config.metrics.start_pushers(cdf)

    with TimeSeriesUploadQueue(
        cdf,
        post_upload_function=state_store.post_upload_handler(),
        max_upload_interval=config.extractor.upload_interval,
        trigger_log_level="INFO",
        thread_name="CDF-Uploader",
    ) as upload_queue:
        # Fill in gap in data between end of last run and now
        logger.info("Starting frontfiller")
        frontfill(upload_queue, frost, weather_stations, config, state_store)

        # Start streaming live data
        logger.info("Starting streamer")
        extractor = Streamer(upload_queue, stop, frost, weather_stations, config)
        Thread(target=extractor.run, name="Streamer").start()

        stop.wait()

    state_store.synchronize()

    if config.metrics:
        config.metrics.stop_pushers()

    logger.info("Extractor end")
