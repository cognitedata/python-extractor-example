from typing import Dict, List, Union

import ujson as ujson
from cognite.client.data_classes import TimeSeries
from requests import Response, Session, adapters  # type: ignore


class IceCreamFactoryAPI:
    """Class for Ice Cream Factory API."""

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.adapter = adapters.HTTPAdapter(max_retries=3)
        self.session = Session()
        self.session.mount("https://", self.adapter)

    def get_response(
            self, headers: Dict[str, str], url_suffix: str, params: Dict[str, Union[str, int, float]] = {}
    ) -> Response:
        """
        Get response from API.

        Args:
            headers: request header
            url_suffix: string to add to base url
            params: query parameters
        """

        response = self.session.get(f"{self.base_url}/{url_suffix}", headers=headers, timeout=40, params=params)
        response.raise_for_status()
        return response

    def get_csv(self, url_suffix: str) -> str:
        """
        Get csv file.

        Args:
            url_suffix: url suffix to the csv endpoint
        """
        response = self.get_response(headers={"Accept": "*/*"}, url_suffix=url_suffix)
        csv = response.content.decode("utf-8")
        return csv

    def get_timeseries_list_for_sites(self, source: str, sites: List[str]) -> List[TimeSeries]:
        """
        Get list of unique timeseries for a given source for sites given in list.

        Args:
            source: data source to get timeseries for
            sites: sites to get timeseries for
        """
        response = self.get_response(headers={}, url_suffix=f"timeseries/{source}")
        timeseries_list = []
        timeseries_ext_ids = set()
        for timeseries in ujson.loads(response.content):
            if timeseries["metadata"]["site"] in sites and timeseries["external_id"] not in timeseries_ext_ids:
                timeseries_list.append(
                    TimeSeries(
                        name=timeseries["name"],
                        external_id=timeseries["external_id"],
                        description=timeseries["description"],
                        is_string=timeseries["is_string"],
                        is_step=timeseries["is_step"],
                        metadata=timeseries["metadata"],
                    )
                )
                timeseries_ext_ids.add(timeseries["external_id"])

        return timeseries_list

    def get_oee_timeseries_datapoints(
            self, timeseries_ext_id: str, start: Union[str, int, float], end: Union[str, int, float]
    ):
        """
        Get datapoints for a timeseries external id. This will also return datapoints for an associated timeseries

        (e.g. request for external id "HPM2C561:planned_status" will return datapoints for "HPM2C561:planned_status" AND
        "HPM2C561:status". Similar, request for timeseries with external id "HPM2C561:count" will return datapoints for
        "HPM2C561:count" AND ""HPM2C561:good").

        Args:
            timeseries_ext_id: external id of timeseries to get datapoints for
            start: start for datapoints (UNIX timestamp (int, float) or string with format 'YYYY-MM-DD HH:MM')
            end: end for datapoints (UNIX timestamp (int, float) or string with format 'YYYY-MM-DD HH:MM')
        """
        params = {"start": start, "end": end, "external_id": timeseries_ext_id}
        response = self.get_response(headers={}, url_suffix="datapoints/oee", params=params)

        datapoints_dict = ujson.loads(response.content)
        datapoints_to_upload = {}

        for timeseries in datapoints_dict:
            ts_datapoints = datapoints_dict[timeseries]
            # convert timestamp to ms (*1000) for CDF uploads
            datapoints_to_upload[timeseries] = [(dp[0] * 1000, dp[1]) for dp in ts_datapoints]

        return datapoints_to_upload
