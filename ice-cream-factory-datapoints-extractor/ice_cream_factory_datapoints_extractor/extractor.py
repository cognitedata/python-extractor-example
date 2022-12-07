import argparse
import logging
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event
from typing import List

from cognite.client import CogniteClient
from cognite.client.data_classes import DataSet
from cognite.client.data_classes import TimeSeries
from cognite.extractorutils import Extractor
from cognite.extractorutils.statestore import AbstractStateStore
from cognite.extractorutils.uploader import TimeSeriesUploadQueue
from cognite.extractorutils.util import ensure_time_series

from ice_cream_factory_datapoints_extractor.config import IceCreamFactoryConfig
from ice_cream_factory_datapoints_extractor.datapoints_backfiller import Backfiller
from ice_cream_factory_datapoints_extractor.datapoints_streamer import Streamer
from ice_cream_factory_datapoints_extractor.ice_cream_factory_api import IceCreamFactoryAPI


def timeseries_updates(
        timeseries_list: List[TimeSeries], config: IceCreamFactoryConfig, client: CogniteClient
) -> List[TimeSeries]:
    """
    Update Timeseries object with dataset_id and asset_id. This is so non-existing timeseries get created with
    the needed data in the ensure_time_series function from extractorutils.

    Args:
        timeseries_list: List of timeseries
        config: Config data for this extractor
        client: Cognite client

    Returns:
        updated_timeseries_list: List of updated timeseries
    """

    asset_ext_ids_set = set([ts.external_id.split(":")[0] for ts in timeseries_list])

    # get asset data from CDF
    cdf_assets = client.assets.retrieve_multiple(external_ids=list(asset_ext_ids_set), ignore_unknown_ids=True)
    asset_ext_id_to_id_dict = {asset.external_id: asset.id for asset in cdf_assets}
    try:
        oee_timeseries_dataset_id = client.data_sets.retrieve(external_id=config.oee_timeseries_dataset_ext_id).id
    except AttributeError:
        logging.info("Could not find existing dataset. Have you run bootstrap cli?")
        raise

    updated_timeseries_list: List[TimeSeries] = []
    for timeseries in timeseries_list:
        timeseries.data_set_id = oee_timeseries_dataset_id
        timeseries.asset_id = asset_ext_id_to_id_dict.get(timeseries.external_id.split(":")[0])
        updated_timeseries_list.append(timeseries)

    return updated_timeseries_list


def run_extractor(
        cognite: CogniteClient, states: AbstractStateStore, config: IceCreamFactoryConfig, stop_event: Event
) -> None:
    """
    Run extractor and extract datapoints for timeseries for sites given in config.

    Args:
        cognite: Initialized cognite client object
        states: Initialized state store object
        config: Configuration parameters
        stop_event: Cancellation token, will be set when an interrupt signal is sent to the extractor process
    """
    logging.info("Starting Ice Cream Factory datapoints extractor")
    ice_cream_api = IceCreamFactoryAPI(base_url=config.api.url)

    sites = ",".join(config.api.sites)
    logging.info(f"Getting OEE timeseries data for the sites {sites}")
    oee_timeseries_list = ice_cream_api.get_timeseries_list_for_sites(source="oee", sites=config.api.sites)

    timeseries_list = timeseries_updates(timeseries_list=oee_timeseries_list, config=config, client=cognite)

    logging.info(f"Ensuring that {len(timeseries_list)} time series exist in CDF")
    # If timeseries don't exist in CDF already, they will be created
    ensure_time_series(cognite, timeseries_list)

    # Only request datapoints for timeseries with count/planned_status in external id.
    # Datapoints for the corresponding good/status timeseries will be returned when querying for count/status timeseries
    # The corresponding timeseries will be uploaded to queue and backfilled
    timeseries_to_query = [
        ts for ts in timeseries_list if ("count" in ts.external_id or "planned_status" in ts.external_id)
    ]

    clean_uploader_queue = TimeSeriesUploadQueue(
        cognite,
        post_upload_function=states.post_upload_handler(),
        max_upload_interval=config.extractor.upload_interval,
        max_queue_size=50_000,
        trigger_log_level="INFO",
        thread_name="CDF-Uploader",
    )

    def chunks(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    futures = []
    with clean_uploader_queue as queue:
        with ThreadPoolExecutor(thread_name_prefix="Data",
                                max_workers=config.extractor.parallelism * 2) as executor:
            if config.backfill.enabled:
                logging.info(f"Starting backfiller. Back-filling for {config.backfill.history_days} days of data")

                for i, batch in enumerate(chunks(timeseries_to_query, 10)):
                    worker = Backfiller(queue, stop_event, ice_cream_api, batch, config, states)
                    futures.append(executor.submit(worker.run))

            if config.frontfill.enabled:
                logging.info(f"Starting frontfiller...")

                for i, batch in enumerate(chunks(timeseries_to_query, 10)):
                    worker = Streamer(queue, stop_event, ice_cream_api, batch, config, states)
                    futures.append(executor.submit(worker.run))

    for future in as_completed(futures):
        future.result()

    queue.upload()  # Ensure leftovers are complete

    if config.frontfill.enabled and not config.backfill.enabled:
        fake_state_ext_id = "fake_failure_counter"
        low, high = states.get_state(fake_state_ext_id)
        if low is None:
            low = 0

        states.set_state(fake_state_ext_id, low + 1, None)

        if low % random.randint(15, 20) == 0:
            raise NotImplementedError("This is a synthetic error. Data was extracted successfully, but monitoring "
                                      "and extraction pipelines should be triggered for DEMO purposes")


def main(config_file_path: str = "extractor_config.yaml") -> None:
    """
    Main entrypoint.
    """
    with Extractor(
            name="datapoints_rest_extractor",
            description="An extractor that ingest datapoints from the Ice Cream Factory API to CDF clean",
            config_class=IceCreamFactoryConfig,
            version="1.0",
            config_file_path=config_file_path,
            run_handle=run_extractor,
    ) as extractor:
        extractor.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run an extractor")
    parser.add_argument(
        "-c",
        "--config",
        dest="config_path",
        type=str,
        help="File containing the configuration for a job",
    )

    args = parser.parse_args()

    main(config_file_path=args.config_path)
