from dataclasses import dataclass
from typing import List


@dataclass
class Level:
    levelType: str
    unit: str
    value: float


@dataclass
class Observation:
    elementId: str
    value: float
    unit: str
    level: Level
    timeOffset: str
    timeResolution: str
    timeSeriesId: int
    performanceCategory: str
    exposureCategory: str
    qualityCode: int


@dataclass
class TimedObservations:
    sourceId: str
    referenceTime: str
    observations: List[Observation]


@dataclass
class WeatherResponse:
    data: List[TimedObservations]
    queryTime: float
    currentItemCount: int
    itemsPerPage: int
    offset: int
    totalItemCount: int
    currentLink: str
