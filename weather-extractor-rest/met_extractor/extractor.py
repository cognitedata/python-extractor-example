import logging
from typing import Iterable, List, Optional

import arrow
from cognite.extractorutils.rest import RestExtractor
from cognite.extractorutils.rest.http import HttpCall, HttpUrl
from cognite.extractorutils.rest.types import InsertDatapoints
from cognite.extractorutils.statestore import NoStateStore

from met_extractor import __version__
from met_extractor.dto import WeatherResponse

extractor = RestExtractor(
    name="met_extractor",
    description="A reimplementation of the MET extractor using the Cognite REST Extension for extractor-utils",
    version=__version__,
    base_url="https://frost.met.no/",
)


def handle_data(response: WeatherResponse) -> Iterable[InsertDatapoints]:
    external_id_prefix = RestExtractor.get_current_config().cognite.external_id_prefix

    for station in response.data:
        ts = int(arrow.get(station.referenceTime).float_timestamp * 1000)
        # Each observation might have multiple values, at different sensor heights, pick lowest
        value = min(station.observations, key=lambda obs: obs.level.value).value

        yield InsertDatapoints(external_id=f"{external_id_prefix}{station.sourceId}", datapoints=[(ts, value)])


@extractor.get(
    "observations/v0.jsonld?sources=SN18700:0,SN50539:0&referencetime=latest&elements=air_temperature",
    response_type=WeatherResponse,
    interval=60,
    name="live",
)
def get_current_temp(response: WeatherResponse) -> Iterable[InsertDatapoints]:
    return handle_data(response)


class BackfillPaginator:
    def __init__(self):
        self.current_states = NoStateStore()

    def get_state(self, tags: List[str]) -> arrow.Arrow:
        state_store = RestExtractor.get_current_statestore() or NoStateStore()
        stored_state = state_store.get_state(
            [RestExtractor.get_current_config().cognite.external_id_prefix + tag for tag in tags]
        )

        states = [s[0] for s in stored_state if s[0] is not None]
        intermittent_states = self.current_states.get_state(tags)
        states.extend([s[0] for s in intermittent_states if s[0] is not None])

        if len(states) > 0:
            cdf_timestamp = int(min(states))
            return arrow.get(cdf_timestamp / 1000)
        else:
            return arrow.get()

    def __call__(self, previous_call: HttpCall) -> HttpUrl:
        tags = previous_call.url.query["sources"].split(",")
        state = self.get_state(tags)
        from_time = state.shift(days=-7)

        for tag in tags:
            self.current_states.expand_state(external_id=tag, low=from_time.int_timestamp * 1000)

        url = previous_call.url
        url.query["referencetime"] = f"{from_time.strftime('%Y-%m-%dT%H:%M:%S')}/{state.strftime('%Y-%m-%dT%H:%M:%S')}"
        return url


@extractor.get(
    f"observations/v0.jsonld?sources=SN18700:0,SN50539:0&referencetime={arrow.get().shift(days=-7).strftime('%Y-%m-%dT%H:%M:%S')}/{arrow.get().strftime('%Y-%m-%dT%H:%M:%S')}&elements=air_temperature",
    response_type=WeatherResponse,
    next_page=BackfillPaginator(),
    name="backfill",
)
def backfill(response: WeatherResponse) -> Iterable[InsertDatapoints]:
    return handle_data(response)


class FrontfillPaginator:
    def __init__(self):
        self.current_states = NoStateStore()

    def get_state(self, tags: List[str]) -> arrow.Arrow:
        state_store = RestExtractor.get_current_statestore() or NoStateStore()
        stored_state = state_store.get_state(
            [RestExtractor.get_current_config().cognite.external_id_prefix + tag for tag in tags]
        )

        states = [s[1] for s in stored_state if s[1] is not None]
        intermittent_states = self.current_states.get_state(tags)
        states.extend([s[1] for s in intermittent_states if s[1] is not None])

        if len(states) > 0:
            cdf_timestamp = int(max(states))
            return arrow.get(cdf_timestamp / 1000)
        else:
            return arrow.get()

    def __call__(self, previous_call: HttpCall) -> Optional[HttpUrl]:
        tags = previous_call.url.query["sources"].split(",")
        state = self.get_state(tags)

        if state > arrow.get():
            logging.getLogger().info("Frontfill done")
            return None

        to_time = state.shift(days=7)

        for tag in tags:
            self.current_states.expand_state(external_id=tag, high=to_time.int_timestamp * 1000)

        url = previous_call.url
        url.query["referencetime"] = f"{state.strftime('%Y-%m-%dT%H:%M:%S')}/{to_time.strftime('%Y-%m-%dT%H:%M:%S')}"
        return url


@extractor.get(
    "observations/v0.jsonld?sources=SN18700:0,SN50539:0&referencetime=latest&elements=air_temperature",
    response_type=WeatherResponse,
    next_page=FrontfillPaginator(),
    name="frontfill",
)
def frontfill(response: WeatherResponse) -> Iterable[InsertDatapoints]:
    return handle_data(response)
