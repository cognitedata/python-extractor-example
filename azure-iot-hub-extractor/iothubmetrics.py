from cognite.extractorutils.metrics import BaseMetrics
from prometheus_client import Counter


class Metrics(BaseMetrics):
    """
    A collection of metrics for the Azure IOT hub extractor
    """

    def __init__(self):
        super(Metrics, self).__init__("iothub_extractor", "0.1.0")

        self.messages_consumed = Counter(
            "iothub_messages_consumed", "Messages Consumed"
        )
        self.iouthub_timeseries_ensured = Counter(
            "iouthub_timeseries_ensured", "Time series ensured"
        )
        self.datapoints_written = Counter(
            "iothub_datapoints_written", "Datapoints written"
        )
