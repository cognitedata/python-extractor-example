import csv
import logging
import sys
from concurrent.futures.thread import ThreadPoolExecutor
from typing import List
from cognite import client
from cognite.client import CogniteClient

from cognite.client.data_classes.time_series import TimeSeries
from cognite.extractorutils.configtools import load_yaml
from cognite.extractorutils.uploader import TimeSeriesUploadQueue
from cognite.extractorutils.util import ensure_time_series

# Local imports from this folder
from iothubconfig import IotHubConfig
from iothubmetrics import Metrics

# Azure IOT Hub
from azure.eventhub import TransportType
from azure.eventhub import EventHubConsumerClient


class IotHubExtractor:
    """
    Main extractor class

    Args:
        queue: Upload queue to use
        metrics: Collection of metrics to use
    """

    def __init__(self, cdf_client: CogniteClient, config: IotHubConfig):
        self.cdf_client = cdf_client
        self.logger = logging.getLogger(__name__)
        self.asset_id = None
        self.time_series_ensured = set()

        self.queue = TimeSeriesUploadQueue(
            cdf_client,
            max_queue_size=config.extractor.upload_queue_size,
            max_upload_interval=config.extractor.upload_interval,
            create_missing=True,
            post_upload_function=self.upload_callback,
        )

    def run(self, config: IotHubConfig) -> None:
        """
        Process queue and upload to CDF

        Args:
            config: Configuration parameters
        """

        CONNECTION_STR = f"Endpoint={config.azureiothub.eventhub_compatible_endpoint}/;SharedAccessKeyName=service;SharedAccessKey={config.azureiothub.iot_sas_key};EntityPath={config.azureiothub.eventhub_compatible_path}"

        client = EventHubConsumerClient.from_connection_string(
            conn_str=CONNECTION_STR,
            consumer_group="$default",
            # transport_type=TransportType.AmqpOverWebsocket,  # uncomment it if you want to use web socket
            # http_proxy={  # uncomment if you want to use proxy
            #     'proxy_hostname': '127.0.0.1',  # proxy hostname.
            #     'proxy_port': 3128,  # proxy port.
            #     'username': '<proxy user name>',
            #     'password': '<proxy password>'
            # }
        )

        self.asset_id = cdf_client.assets.retrieve(
            external_id=config.azureiothub.iot_root
        ).id

        try:
            with client:
                client.receive_batch(
                    on_event_batch=self.on_event_batch, on_error=self.on_error
                )
        except KeyboardInterrupt:
            print("Receiving has stopped.")

    # Define callbacks to process events
    def on_event_batch(self, partition_context, events):
        for event in events:
            values = event.body_as_json()
            device = event.system_properties[b"iothub-connection-device-id"].decode(
                "utf-8"
            )
            for key in values.keys():
                ext_id = f"{device}_{key}"

                if not ext_id in self.time_series_ensured:
                    ensure_time_series(
                        self.cdf_client,
                        [
                            TimeSeries(
                                external_id=ext_id,
                                name=f"{device} {key}",
                                asset_id=self.asset_id,
                            )
                        ],
                    )
                    self.time_series_ensured.add(ext_id)

                timestamp = event.system_properties[b"iothub-enqueuedtime"]

                self.queue.add_to_upload_queue(
                    external_id=ext_id, datapoints=[(timestamp, values[key])],
                )

        self.queue.upload()  # upload to CDF
        partition_context.update_checkpoint()

    def on_error(self, partition_context, error):
        # Put your code here. partition_context can be None in the on_error callback.
        if partition_context:
            print(
                "An exception: {} occurred during receiving from Partition: {}.".format(
                    partition_context.partition_id, error
                )
            )
        else:
            print(
                "An exception: {} occurred during the load balance process.".format(
                    error
                )
            )

    def upload_callback(self, uploaded_datapoints):
        logging.getLogger(__name__).info(
            f"Uploaded {len(uploaded_datapoints)} datapoimts to CDF"
        )


if __name__ == "__main__":
    with open(sys.argv[1]) as config_file:
        config = load_yaml(config_file, IotHubConfig)

    config.logger.setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("Starting example Azure IOT Hub extractor")

    cdf_client = config.cognite.get_cognite_client("azure-iot-extractor")

    extractor = IotHubExtractor(cdf_client, config)
    extractor.run(config)

    logger.info("Extractor end")
