from dataclasses import dataclass
from typing import List, Optional

from cognite.extractorutils.configtools import (
    BaseConfig,
    MetricsConfig,
    RawDestinationConfig,
)


@dataclass
class ExtractorConfig:
    """
    Configuration for the running extractor, so any performance tuning parameters should go here
    """

    upload_queue_size: int = 10
    upload_interval: int = 30
    parallelism: int = 1


@dataclass
class EventHubConfig:
    """
    Source configuration, Iot Hub connection parameters
    """

    # Event Hub-compatible endpoint
    # az iot hub show --query properties.eventHubEndpoints.events.endpoint --name {your IoT Hub name}
    eventhub_compatible_endpoint: str
    # Event Hub-compatible name
    # az iot hub show --query properties.eventHubEndpoints.events.path --name {your IoT Hub name}
    eventhub_compatible_path: str

    # Primary key for the "service" policy to read messages
    # az iot hub policy show --name service --query primaryKey --hub-name {your IoT Hub name}
    iot_sas_key: str

    # External ID of asset to assign unknown time series
    iot_root: str


@dataclass
class IotHubConfig(BaseConfig):
    """
    Master configuration class, containing everything from the BaseConfig class, in addition to the custom building
    blocks defined above
    """

    metrics: Optional[MetricsConfig]
    azureiothub: EventHubConfig

    extractor: ExtractorConfig = ExtractorConfig()
