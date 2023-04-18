import dataclasses
import uuid
from datetime import datetime
from enum import Enum
from typing import Any


class EventType(Enum):
    LIKE = 'like'
    DISLIKE = 'dislike'
    VIEW_INCIDENT_RESULTS = 'view-incident-results'
    VIEW_INCIDENT_RESULTS_NA = 'view-incident-results-na'
    VIEW_SIMILAR_INCIDENT = 'view-similar-incident'
    VIEW_SOURCE_INCIDENT = 'view-source-incident'

    def is_sentiment(self, event_type):
        return event_type == self.LIKE or event_type == self.DISLIKE


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
