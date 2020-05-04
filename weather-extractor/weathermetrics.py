from cognite.extractorutils.metrics import BaseMetrics


class Metrics(BaseMetrics):
    def __init__(self):
        super().__init__("weather-extractor", "0.1.0")
