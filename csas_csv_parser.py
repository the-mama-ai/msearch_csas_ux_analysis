import csv
import dataclasses
from datetime import datetime, date
import uuid
from enum import Enum
from pprint import pprint
from typing import Any

import httpx


class EventType(Enum):
    LIKE = 'like'
    DISLIKE = 'dislike'
    VIEW_INCIDENT_RESULTS = 'view-incident-results'
    VIEW_INCIDENT_RESULTS_NA = 'view-incident-results-na'
    VIEW_SIMILAR_INCIDENT = 'view-similar-incident'
    VIEW_SOURCE_INCIDENT = 'view-source-incident'


@dataclasses.dataclass(slots=True, frozen=True)
class Event:
    """ One row in csas_file.csv """
    source_ticket_id: str
    similar_ticket_id: str
    similar_tickets_ids: list[str]
    rank: int
    client_id: uuid.UUID
    timestamp: datetime
    event_type: EventType
    assignment_group: str

    def to_dict(self) -> dict[str, Any]:
        attributes_with_values: dict[str, Any] = {}
        for field in self.__dataclass_fields__:
            attributes_with_values[field] = getattr(self, field)
        return attributes_with_values


class Field(int, Enum):
    """ Column type, indexed from 0, in the csas_file.csv """
    TIMESTAMP = 1
    EVENT_TYPE = 2
    EVENT_ID = 3
    SOURCE_TICKET_ID = 4
    ASSIGNMENT_GROUP = 5
    SIMILAR_TICKET_ID = 6
    SIMILAR_TICKET_IDS = 7
    RANK = 8
    CLIENT_ID = 10


def parse_all_events_from_csv(csv_events_file_path: str) -> (list[Event]):
    events: list[Event] = []

    with open(csv_events_file_path, 'r') as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            if row[0] == '':
                continue

            similar_tickets_ids: list[str] = row[Field.SIMILAR_TICKET_IDS] \
                .replace(',', '').replace('[', '').replace(']', '').replace("'", '').split(' ')

            rank = row[Field.RANK] or 0
            client_id = uuid.UUID(row[Field.CLIENT_ID]) if not row[Field.CLIENT_ID] == 'n/a' else None

            events.append(Event(source_ticket_id=row[Field.SOURCE_TICKET_ID],
                                similar_ticket_id=row[Field.SIMILAR_TICKET_ID],
                                similar_tickets_ids=similar_tickets_ids,
                                rank=int(float(rank)) - 1,
                                timestamp=datetime.strptime(row[Field.TIMESTAMP], '%Y-%m-%d %H:%M:%S'),
                                client_id=client_id,
                                event_type=EventType(row[Field.EVENT_TYPE]),
                                assignment_group=row[Field.ASSIGNMENT_GROUP]))
        return events


# def retrieve_incident(incident_id: str):
#     collection = 'csas_inc_rules1_age'
#     rest_endpoint_url = f'https://api.msearch-dev.ch.themama.ai/msearch/collections/{collection}/{incident_id}'
#     with httpx.Client() as client:
#         request = client.get(rest_endpoint_url, auth=httpx.BasicAuth('csas', 'Db8KwhKLuJC*'))


def get_event_type_counts(events: list[Event]) -> dict[EventType, int]:
    event_counts = {event_type: 0 for event_type in EventType}
    for event in events:
        event_counts[event.event_type] += 1
    return event_counts


def filter_events_by_time(events: list[Event], date_from: date) -> list[Event]:
    return [event for event in events if event.timestamp.date() >= date_from]


def get_unique_assignment_groups(csv_events_file_path: str) -> list[str]:
    unique_assignment_groups: set[str] = set()

    with open(csv_events_file_path, 'r') as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            if row[0] == '':
                continue
            unique_assignment_groups.add(row[Field.ASSIGNMENT_GROUP])

    return unique_assignment_groups


if __name__ == '__main__':
    csv_file_path = 'ui_logs_2023-03-15 22_12_03.708255.csv'
    events: list[Event]
    event_counts: dict[str, int]
    events = parse_all_events_from_csv(csv_file_path)
    events = filter_events_by_time(events, date_from=date(2023, 3, 6))
    print(events[0].to_dict())
    #
    # retrieve_incident('INC39611601')
