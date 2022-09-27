from dataclasses import dataclass
from typing import List

from cognite.extractorutils.configtools import BaseConfig, RawDestinationConfig


@dataclass
class ExtractorConfig:
    """
    Configuration for the running extractor, so any performance tuning parameters should go here
    """
    upload_queue_size: int = 50000
    parallelism: int = 10


@dataclass
class FileConfig:
    """
    Source configuration, describing a CSV file, and where to put it in CDF
    """
    path: str
    key_column: str
    destination: RawDestinationConfig


@dataclass
class CsvConfig(BaseConfig):
    """
    Master configuration class, containing everything from the BaseConfig class, in addition to the custom building
    blocks defined above
    """
    files: List[FileConfig]
    extractor: ExtractorConfig = ExtractorConfig()
