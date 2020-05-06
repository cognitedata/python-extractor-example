from dataclasses import dataclass
from typing import List, Optional, Union

from cognite.extractorutils.configtools import BaseConfig, LocalStateStoreConfig, MetricsConfig, StateStoreConfig
from cognite.extractorutils.statestore import LocalStateStore


@dataclass
class FrostConfig:
    client_id: str
    elements: List[str]


@dataclass
class LocationConfig:
    name: Optional[str]
    longitude: Optional[float]
    latitude: Optional[float]
    station_id: Optional[str]

    def validate(self) -> bool:
        """
        Tests if either station id is used, or long/lat is used, but not both.
        """
        station_is_set = self.station_id is not None
        geometry_is_set = self.name is not None and self.longitude is not None and self.latitude is not None
        both_is_set = station_is_set and geometry_is_set

        return (station_is_set or geometry_is_set) and not both_is_set


@dataclass
class ExtractorConfig:
    state_store: StateStoreConfig = StateStoreConfig(local=LocalStateStoreConfig(path="states.json"), raw=None)
    create_assets: bool = False
    upload_interval: int = 10
    parallelism: int = 10


@dataclass
class BackfillConfig:
    backfill_to: Union[str, float, int]


@dataclass
class WeatherConfig(BaseConfig):
    metrics: Optional[MetricsConfig]
    frost: FrostConfig
    locations: List[LocationConfig]
    backfill: Optional[BackfillConfig]

    extractor: ExtractorConfig = ExtractorConfig()
