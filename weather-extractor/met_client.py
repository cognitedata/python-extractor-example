from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Union

import arrow
import requests
from cognite.client.data_classes import Asset


@dataclass
class WeatherStation:
    name: str
    id: str
    country: str
    longitude: float
    latitude: float

    def __hash__(self) -> int:
        return self.id.__hash__()


class FrostApi:
    """
    A small 'SDK' for the Frost API containing the functionality required by our weather extractor.

    Args:
        client_id: Frost credentials
    """

    def __init__(self, client_id: str):
        self.client_id = client_id

    def _station_from_response(self, json_response: Dict[str, Any]) -> WeatherStation:
        """
        Create a WeatherStation object based on the response from Frost.

        Args:
            json_response: JSON response from the api, decoded to a dict

        Returns:
            A WeatherStation object
        """

        data = json_response["data"][0]

        return WeatherStation(
            name=data["name"],
            id=data["id"],
            country=data["country"],
            longitude=data["geometry"]["coordinates"][0],
            latitude=data["geometry"]["coordinates"][0],
        )

    def get_closest_station(self, longitude: float, latitude: float) -> WeatherStation:
        """
        Query the Frost API for the weather station closest to a given point.

        Args:
            longitude: Longitude of point (as a decimal floating point number)
            latitude: Latitude of point (as a decimal floating point number)

        Returns:
            A WeatherStation object
        """
        response = requests.get(
            "https://frost.met.no/sources/v0.jsonld",
            {"geometry": f"nearest(POINT({longitude} {latitude}))", "nearestmaxcount": 1},
            auth=(self.client_id, ""),
        )
        response.raise_for_status()

        return self._station_from_response(response.json())

    def get_station(self, station_id: str) -> WeatherStation:
        """
        Query the Frost API for data on a weather station given the station's ID

        Args:
            station_id: Station ID

        Returns:
            WeatherStation object
        """
        response = requests.get(
            "https://frost.met.no/sources/v0.jsonld", {"ids": station_id}, auth=(self.client_id, ""),
        )
        response.raise_for_status()

        return self._station_from_response(response.json())

    def get_current(self, station: WeatherStation, elements: List[str]) -> Dict[str, Tuple[int, float]]:
        """
        Get the current values for sensors at a weather station

        Args:
            station: The weather station
            elements: The elements to get data for (e.g. air_temperature, wind_speed, etc)

        Returns:
            Map from element to datapoint (tuple of UTC microsecond timestamp and value)
        """
        response = requests.get(
            "https://frost.met.no/observations/v0.jsonld",
            {"sources": station.id, "elements": ",".join(elements), "referencetime": "latest"},
            auth=(self.client_id, ""),
        )
        response.raise_for_status()

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
        """
        Get the historical values for sensors at a weather station

        Args:
            station: The weather station
            elements: The elements to get data for (e.g. air_temperature, wind_speed, etc)
            from_time: Lower boundary for time gap to query
            to_time: Upper boundary for time gap to query

        Returns:
            Map from element to a list of datapoints (each datapoint is a tuple of UTC microsecond timestamp and value)
        """
        response = requests.get(
            "https://frost.met.no/observations/v0.jsonld",
            {
                "sources": station.id,
                "elements": ",".join(elements),
                "referencetime": f"{from_time.isoformat()}/{to_time.isoformat()}",
            },
            auth=(self.client_id, ""),
        )
        response.raise_for_status()

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
