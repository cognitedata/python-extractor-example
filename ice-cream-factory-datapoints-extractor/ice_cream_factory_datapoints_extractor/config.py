from dataclasses import dataclass
from typing import List

from cognite.extractorutils.configtools import BaseConfig, RawStateStoreConfig, StateStoreConfig


@dataclass
class ApiConfig:
    url: str
    sites: List[str]


@dataclass
class ExtractorConfig:
    state_store: StateStoreConfig = StateStoreConfig(
        local=None,
        raw=RawStateStoreConfig(
            database="src:005:oee:db:state", table="timeseries_datapoints_states", upload_interval=5
        ),
    )

    create_assets: bool = False
    upload_interval: int = 5  # Automatically trigger an upload each m seconds when run as a thread
    parallelism: int = 2


@dataclass
class BackfillConfig:
    backfill_min: int


@dataclass
class IceCreamFactoryConfig(BaseConfig):
    api: ApiConfig
    backfill: BackfillConfig
    oee_timeseries_dataset_ext_id: str  # ext id of dataset for oee timeseries. Used to populate timeseries in the correct dataset
    extractor: ExtractorConfig = ExtractorConfig()
