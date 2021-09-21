from cognite.extractorutils.metrics import BaseMetrics, safe_get
from prometheus_client import Counter

from . import __version__

class Metrics(BaseMetrics):
    """
    A collection of metrics for the example CSV extractor
    """

    def __init__(self):
        super(Metrics, self).__init__("csv_extractor", __version__)

        self.files_started = Counter("csv_extractor_files_started", "Extractions started")
        self.files_success = Counter("csv_extractor_files_success", "Extractions finished")
        self.files_failed = Counter("csv_extractor_files_failed", "Extractions failed")
        self.rows_fetched = Counter("csv_extractor_rows_fetched", "Number of rows fetched")
