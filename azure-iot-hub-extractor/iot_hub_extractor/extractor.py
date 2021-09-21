import logging

from azure.eventhub import EventHubConsumerClient
from cognite.client.data_classes.time_series import TimeSeries
from cognite.extractorutils import Extractor
from cognite.extractorutils.metrics import safe_get
from cognite.extractorutils.uploader import TimeSeriesUploadQueue
from cognite.extractorutils.util import ensure_time_series

from . import __version__
from .config import IotHubConfig
from .metrics import Metrics


class IotHubExtractor(Extractor):
    """
    Main extractor class

    Args:
        queue: Upload queue to use
        metrics: Collection of metrics to use
    """

    def __init__(self):
        super().__init__(
            name="iothub_extractor",
            description="An Azure IOT Hub extractor",
            version=__version__,
            config_class=IotHubConfig,
            metrics=safe_get(Metrics),
        )
        self.logger = logging.getLogger(__name__)
        self.asset_id = None
        self.time_series_ensured = set()

    def run(self) -> None:
        """
        Process queue and upload to CDF

        Args:
            config: Configuration parameters
        """

        CONNECTION_STR = f"Endpoint={self.config.azureiothub.eventhub_compatible_endpoint}/;SharedAccessKeyName=service;SharedAccessKey={self.config.azureiothub.iot_sas_key};EntityPath={self.config.azureiothub.eventhub_compatible_path}"

        client: EventHubConsumerClient = EventHubConsumerClient.from_connection_string(
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

        self.asset_id = self.cognite_client.assets.retrieve(external_id=self.config.azureiothub.iot_root).id

        try:
            with client:
                client.receive_batch(
                    on_event_batch=self.on_event_batch, on_error=self.on_error,
                )
        except KeyboardInterrupt:
            print("Receiving has stopped.")

    # Define callbacks to process events
    def on_event_batch(self, partition_context, events) -> None:
        for event in events:
            self.metrics.messages_consumed.inc()
            values = event.body_as_json()
            device = event.system_properties[b"iothub-connection-device-id"].decode("utf-8")
            for key in values.keys():
                ext_id = f"{device}_{key}"

                ensure_time_series(
                    self.cognite_client,
                    [TimeSeries(external_id=ext_id, name=f"{device} {key}", asset_id=self.asset_id,)],
                )

                timestamp = event.system_properties[b"iothub-enqueuedtime"]

                self.queue.add_to_upload_queue(
                    external_id=ext_id, datapoints=[(timestamp, values[key])],
                )

        self.queue.upload()  # upload to CDF
        partition_context.update_checkpoint()

    def on_error(self, partition_context, error) -> None:
        # Put your code here. partition_context can be None in the on_error callback.
        if partition_context:
            print(
                "An exception: {} occurred during receiving from Partition: {}.".format(
                    partition_context.partition_id, error
                )
            )
        else:
            print("An exception: {} occurred during the load balance process.".format(error))

    def upload_callback(self, uploaded_datapoints) -> None:
        count = 0
        for entry in uploaded_datapoints:
            count += len(entry["datapoints"])

        logging.getLogger(__name__).info(f"Uploaded {count} datapoints to CDF")
        self.metrics.datapoints_written.inc(count)

    def __enter__(self) -> "IotHubExtractor":
        super(IotHubExtractor, self).__enter__()
        self.queue = TimeSeriesUploadQueue(
            self.cognite_client,
            max_queue_size=self.config.extractor.upload_queue_size,
            max_upload_interval=self.config.extractor.upload_interval,
            post_upload_function=self.upload_callback,
        )
        self.queue.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.queue.__exit__(exc_type, exc_val, exc_tb)
        return super(IotHubExtractor, self).__exit__(exc_type, exc_val, exc_tb)


def main() -> None:
    with IotHubExtractor() as extractor:
        extractor.run()


if __name__ == "__main__":
    main()
