import csv
import logging
import sys
from concurrent.futures.thread import ThreadPoolExecutor
from typing import List

from cognite.client.data_classes import Row
from cognite.extractorutils.configtools import load_yaml
from cognite.extractorutils.uploader import RawUploadQueue

# Local imports from this folder
from csvconfig import CsvConfig, FileConfig
from csvmetrics import Metrics


class CsvExtractor:
    def __init__(self, queue: RawUploadQueue, metrics: Metrics):
        self.queue = queue
        self.metrics = metrics
        self.logger = logging.getLogger(__name__)

    def extract(self, file: FileConfig):
        self.logger.info(f"Extracting content from {file.path} to {file.destination.database}/{file.destination.table}")
        self.metrics.files_started.inc()

        try:
            with open(file.path) as infile:
                reader = csv.DictReader(infile, delimiter=",")

                # Skip header
                next(reader)

                for row in reader:
                    self.queue.add_to_upload_queue(
                        database=file.destination.database,
                        table=file.destination.table,
                        raw_row=Row(key=row[file.key_column], columns=row),
                    )

                self.metrics.rows_fetched.inc()
            self.metrics.files_success.inc()

        except Exception as e:
            self.logger.exception(f"Extraction of {file.path} failed")
            self.metrics.files_failed.inc()

    def run(self, config: CsvConfig):
        # Extract all files, with parallelism as configured
        with ThreadPoolExecutor(
            max_workers=config.extractor.parallelism, thread_name_prefix="CsvExtractor"
        ) as executor:
            for file in config.files:
                executor.submit(self.extract, file)


if __name__ == "__main__":
    with open(sys.argv[1]) as config_file:
        config = load_yaml(config_file, CsvConfig)

    config.logger.setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("Starting example CSV extractor")

    metrics = Metrics()
    cdf_client = config.cognite.get_cognite_client("example-csv-extractor")

    if config.metrics:
        config.metrics.start_pushers(cdf_client)

    def upload_callback(uploaded_rows: List[Row]):
        metrics.rows_uploaded.inc(len(uploaded_rows))
        logging.getLogger(__name__).info(f"Uploaded {len(uploaded_rows)} rows to CDF RAW")

    with RawUploadQueue(cdf_client, max_queue_size=config.extractor.upload_queue_size) as queue:
        extractor = CsvExtractor(queue, metrics)
        extractor.run(config)

    metrics.finish.set_to_current_time()
    if config.metrics:
        config.metrics.stop_pushers()

    logger.info("Extractor end")
