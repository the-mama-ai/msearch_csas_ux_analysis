import dataclasses
from datetime import datetime
from uuid import UUID
import numpy as np
import pandas as pd
from event import Event, EventType

# ======== C O N S T A N T S ==========================
LIKE_WEIGHT = 1
DISLIKE_WEIGHT = 0.67
SIMILAR_VIEW_WEIGHT = 10e-4
# =======================================================

@dataclasses.dataclass
class Session:
    timestamp_start: datetime = None
    sentiment: float = 0
    events: list[Event] = dataclasses.field(default_factory=list)
    only_view: bool = True


def process_sessions(df: pd.DataFrame, events: list[Event]):
    grouped_source_and_user = df.groupby(['source_ticket_id', 'client_id'])

    sessions_sentiment: dict[str, np.ndarray] = {}
    for source_and_user, indices in grouped_source_and_user.indices.items():
        contains_sentiment: bool = False
        for index in indices:
            event: Event = events[index]
            if event.event_type in [EventType.LIKE, EventType.DISLIKE]:
                contains_sentiment = True
                break
        if contains_sentiment:
            sessions_sentiment[source_and_user] = indices

    sessions_only_view: dict[str, np.ndarray] = {}
    for source_and_user, indices in grouped_source_and_user.indices.items():
        contains_view_similar: bool = False
        for index in indices:
            event: Event = events[index]
            if event.event_type in [EventType.LIKE, EventType.DISLIKE]:
                contains_view_similar = False
                break
            if event.event_type == EventType.VIEW_SIMILAR_INCIDENT:
                contains_view_similar = True
        if contains_view_similar:
            sessions_only_view[source_and_user] = indices

    relevant_sessions = sessions_sentiment | sessions_only_view
    sessions: dict[str, Session] = {name: Session() for name in relevant_sessions.keys()}
    for source_and_user, indices in relevant_sessions.items():
        likes: float = 0
        dislikes: float = 0
        views: float = 0

        liked_ids: set[str] = set()
        disliked_ids: set[str] = set()
        similar_view_ids: set[str] = set()
        for index in indices:
            event: Event = events[index]
            match event.event_type:
                case EventType.LIKE:
                    sessions[source_and_user].only_view = False
                    if event.similar_ticket_id not in liked_ids:
                        likes += LIKE_WEIGHT
                        liked_ids.add(event.similar_ticket_id)
                case EventType.DISLIKE:
                    sessions[source_and_user].only_view = False
                    if event.similar_ticket_id not in disliked_ids:
                        dislikes += DISLIKE_WEIGHT
                        disliked_ids.add(event.similar_ticket_id)
                case EventType.VIEW_SIMILAR_INCIDENT:
                    if event.similar_ticket_id not in similar_view_ids:
                        views += SIMILAR_VIEW_WEIGHT
                        similar_view_ids.add(event.similar_ticket_id)
        dislike_sum = dislikes
        if likes > 0 and dislikes > 0:
            dislikes = 0
        sessions[source_and_user].sentiment = float((likes + views - dislikes) / (likes + views + dislike_sum))

        sessions[source_and_user].events += [events[index] for index in indices]
        sessions[source_and_user].timestamp_start = events[indices[0]].timestamp

    return sessions


def sort_sessions_by_time(sessions: dict[str, Session]) -> tuple[str, Session]:
    return sorted(sessions.items(), key=lambda item: item[1].timestamp_start)


def untoggle_events(events: list[Event]):
    # Time locality of session can be in current setup handled by checking 'client_id' which is limited to 1 hour,
    # so no time window has to be used for session/query sentiment.

    @dataclasses.dataclass
    class Client:
        id: UUID
        liked_incident_result: dict[str, list[list[int, Event]]] = dataclasses.field(default_factory=dict)
        disliked_incident_result: dict[str, list[list[int, Event]]] = dataclasses.field(default_factory=dict)

    clients: dict[UUID, Client] = {}
    for index, event in enumerate(events):
        client: Client = clients.get(event.client_id, Client(event.client_id))

        if event.event_type == EventType.LIKE:
            if event.similar_ticket_id not in client.liked_incident_result:
                client.liked_incident_result[event.similar_ticket_id] = []
            client.liked_incident_result[event.similar_ticket_id].append([index, event])

        if event.event_type == EventType.DISLIKE:
            if event.similar_ticket_id not in client.disliked_incident_result:
                client.disliked_incident_result[event.similar_ticket_id] = []
            client.disliked_incident_result[event.similar_ticket_id].append(([index, event]))

        clients[event.client_id] = client

    def did_toggle(toggled_events: list[Event]) -> bool:
        if len(toggled_events) == 0:
            return False
        return len(toggled_events) % 2 == 0

    event_indices_to_remove: list[int] = []

    for client in clients.values():
        for result_id, possibly_toggled_events in client.liked_incident_result.items():
            if did_toggle(possibly_toggled_events):
                print(f"{client.id} : {result_id}  toggled {len(possibly_toggled_events)} likes")
                for index, event in possibly_toggled_events:
                    event_indices_to_remove.append(index)

        for possibly_toggled_events in client.disliked_incident_result.values():
            if did_toggle(possibly_toggled_events):
                print(f"{client.id} : {result_id}  toggled {len(possibly_toggled_events)} dislikes.")
                for index, event in possibly_toggled_events:
                    event_indices_to_remove.append(index)

    untoggled_events = []
    for index, event in enumerate(events):
        if index in event_indices_to_remove:
            continue
        untoggled_events.append(event)

    return untoggled_events
