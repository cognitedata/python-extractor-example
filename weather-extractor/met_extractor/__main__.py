import logging
from threading import Event, Thread
from typing import Dict, List, Optional
from time import sleep

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, TimeSeries
from cognite.extractorutils import Extractor
from cognite.extractorutils.base import ReloadConfigAction
from cognite.extractorutils.statestore import AbstractStateStore
from cognite.extractorutils.uploader import TimeSeriesUploadQueue
from cognite.extractorutils.util import ensure_time_series, ensure_assets

from . import __version__
from .config import LocationConfig, WeatherConfig
from .met_client import FrostApi, WeatherStation
from .streamer import Backfiller, Streamer, create_external_id, frontfill


def init_stations(locations: List[LocationConfig], frost: FrostApi) -> List[WeatherStation]:
    """
    Create WeatherStation objects based on the location list in the config

    Args:
        locations: List of location configurations
        frost: Frost API

    Returns:
        List of initialized WeatherStations
    """
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
    """
    Create TimeSeries Objects (without creating them in CDF) for all the sensors at all the weather stations configured.

    Args:
        weather_stations: List of weather stations to track
        config: Configuration parameters, among other containing the list of elements to track
        assets: (Optional) Dictionary from WeatherStation object to of asset ID. If configured to create assets, the
                time series will be associated with an asset ID.

    Returns:
        List of TimeSeries objects
    """
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

            args["data_set_id"] = config.cognite.get_extraction_pipeline(config.cognite.get_cognite_client("")).data_set_id

            time_series.append(TimeSeries(**args))

    return time_series


def create_assets(
    weather_stations: List[WeatherStation], config: WeatherConfig, cdf: CogniteClient
) -> Dict[WeatherStation, int]:
    """
    Create assets in CDF for all WeatherStation objects

    Args:
        weather_stations: List of weather stations
        config: Config parameters
        cdf: Cognite client

    Returns:
        Mapping from WeatherStation object to (internal) asset ID in CDF
    """
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
                data_set_id=config.cognite.get_extraction_pipeline(cdf).data_set_id
            )
        )

    logger = logging.getLogger(__name__)
    logger.info("Ensuring assets")
    created_assets = ensure_assets(cdf, assets)

    logger.info("Waiting for eventual consistency")
    created_assets = []
    while len(created_assets) != len(assets):
        created_assets = cdf.assets.list(external_id_prefix=config.cognite.external_id_prefix, limit=None)
        sleep(1)
    station_to_asset_id = {}

    logger.info("Creating asset map")
    for asset in created_assets:
        weather_station = [s for s in weather_stations if s.id == asset.metadata["station_id"]][0]
        station_to_asset_id[weather_station] = asset.id

    return station_to_asset_id


def run_extractor(cognite: CogniteClient, states: AbstractStateStore, config: WeatherConfig, stop_event: Event) -> None:
    logger = logging.getLogger(__name__)

    logger.info("Starting example Frost extractor")
    frost = FrostApi(config.frost.client_id)

    logger.info("Getting info about weather stations")
    weather_stations = init_stations(config.locations, frost)

    if config.extractor.create_assets:
        assets = create_assets(weather_stations, config, cognite)
    else:
        assets = None

    time_series = list_time_series(weather_stations, config, assets)

    logger.info(f"Ensuring that {len(time_series)} time series exist in CDF")
    ensure_time_series(cognite, time_series)

    with TimeSeriesUploadQueue(
        cognite,
        post_upload_function=states.post_upload_handler(),
        max_upload_interval=config.extractor.upload_interval,
        trigger_log_level="INFO",
        thread_name="CDF-Uploader",
    ) as upload_queue:
        if config.backfill:
            logger.info("Starting backfiller")
            backfiller = Backfiller(upload_queue, stop_event, frost, weather_stations, config, states)
            Thread(target=backfiller.run, name="Backfiller").start()

        # Fill in gap in data between end of last run and now
        logger.info("Starting frontfiller")
        frontfill(upload_queue, frost, weather_stations, config, states)

        # Start streaming live data
        logger.info("Starting streamer")
        streamer = Streamer(upload_queue, stop_event, frost, weather_stations, config)
        Thread(target=streamer.run, name="Streamer").start()

        stop_event.wait()


def main() -> None:
    with Extractor(
        name="weather_extractor",
        description="An extractor gathering weather data from the Norwegian Meteorological Institute",
        config_class=WeatherConfig,
        version=__version__,
        run_handle=run_extractor,
        reload_config_action=ReloadConfigAction.SHUTDOWN,
    ) as extractor:
        extractor.run()



if __name__ == "__main__":
    main()
