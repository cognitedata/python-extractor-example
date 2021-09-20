from cognite.extractorutils.metrics import BaseMetrics
from prometheus_client import Counter

from . import __version__


class Metrics(BaseMetrics):
    """
    A collection of metrics for the Azure IOT hub extractor
    """

    def __init__(self):
        super(Metrics, self).__init__("iothub_extractor", __version__)

        self.messages_consumed = Counter("iothub_messages_consumed", "Messages Consumed")
        self.datapoints_written = Counter("iothub_datapoints_written", "Datapoints written")
