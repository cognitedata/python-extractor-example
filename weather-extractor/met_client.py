from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Union

import arrow
import requests
from cognite.client.data_classes import Asset

from weatherconfig import LocationConfig


@dataclass
class WeatherStation:
    name: str
    id: str
    country: str
    longitude: float
    latitude: float

    def to_asset(self) -> Asset:
        pass

    @staticmethod
    def from_location_config(config: LocationConfig) -> "WeatherStation":
        pass

    def __hash__(self) -> int:
        return self.id.__hash__()


class FrostApi:
    """
    A small 'SDK' for the Frost API containing the functionality required by our weather extractor
    """

    def __init__(self, client_id: str):
        self.client_id = client_id

    def _station_from_response(self, json_response: Dict[str, Any]):
        data = json_response["data"][0]

        return WeatherStation(
            name=data["name"],
            id=data["id"],
            country=data["country"],
            longitude=data["geometry"]["coordinates"][0],
            latitude=data["geometry"]["coordinates"][0],
        )

    def get_closest_station(self, longitude: float, latitude: float) -> WeatherStation:
        response = requests.get(
            "https://frost.met.no/sources/v0.jsonld",
            {"geometry": f"nearest(POINT({longitude} {latitude}))", "nearestmaxcount": 1},
            auth=(self.client_id, ""),
        )

        return self._station_from_response(response.json())

    def get_station(self, station_id: str) -> WeatherStation:
        response = requests.get(
            "https://frost.met.no/sources/v0.jsonld", {"ids": station_id}, auth=(self.client_id, ""),
        )

        return self._station_from_response(response.json())

    def get_current(self, station: WeatherStation, elements: List[str]) -> Dict[str, Tuple[int, float]]:
        response = requests.get(
            "https://frost.met.no/observations/v0.jsonld",
            {"sources": station.id, "elements": ",".join(elements), "referencetime": "latest"},
            auth=(self.client_id, ""),
        )

        data_list = response.json()["data"]

        result = {}
        for data in data_list:
            timestamp = int(arrow.get(data["referenceTime"]).float_timestamp * 1000)

            for observation in data["observations"]:
                if observation["elementId"] not in result:
                    result[observation["elementId"]] = (timestamp, observation["value"])

        return result

    def get_historical(
        self, station: WeatherStation, elements: List[str], from_time: arrow.Arrow, to_time: arrow.Arrow
    ) -> Dict[str, List[Tuple[int, float]]]:
        response = requests.get(
            "https://frost.met.no/observations/v0.jsonld",
            {
                "sources": station.id,
                "elements": ",".join(elements),
                "referencetime": f"{from_time.isoformat()}/{to_time.isoformat()}",
            },
            auth=(self.client_id, ""),
        )

        data = response.json()["data"]

        result = defaultdict(list)

        for raw_datapoint in data:
            timestamp = int(arrow.get(raw_datapoint["referenceTime"]).float_timestamp * 1000)
            seen_elements = set()

            for observation in raw_datapoint["observations"]:
                if observation["elementId"] not in seen_elements:
                    seen_elements.add(observation["elementId"])
                    result[observation["elementId"]].append((timestamp, observation["value"]))

        return result
