from cognite.extractorutils.metrics import BaseMetrics
from prometheus_client import Counter


class Metrics(BaseMetrics):
    """
    A collection of metrics for the example CSV extractor
    """

    def __init__(self):
        super(Metrics, self).__init__("csv_extractor", "0.1.0")

        self.files_started = Counter("csv_extractor_files_started", "Extractions started")
        self.files_success = Counter("csv_extractor_files_success", "Extractions finished")
        self.files_failed = Counter("csv_extractor_files_failed", "Extractions failed")
        self.rows_fetched = Counter("csv_extractor_rows_fetched", "Number of rows fetched")
        self.rows_uploaded = Counter("csv_extractor_rows_uploaded", "Number of rows uploaded")
