import csv
import uuid
from enum import Enum
from uuid import UUID

from event import EventType


class Column(int, Enum):
    """ Column type, indexed from 0, in the csas_file.csv """
    TIMESTAMP = 1
    EVENT_TYPE = 2
    SOURCE_TICKET_ID = 3
    SIMILAR_TICKET_ID = 4
    RADEK = 5
    LUDEK = 6
    PABLO = 7
    EVENT_ID = 8
    ASSIGNMENT_GROUP = 9
    SIMILAR_TICKET_IDS = 10
    RANK = 11
    CLIENT_IP = 12
    CLIENT_ID = 13
    UNDO = 14


def parse_sessions_to_remove(dislike_evaluation_csv_path: str) -> (list[tuple[str, UUID]]):
    not_relevant_sessions: list[tuple[str, UUID]] = []

    with open(dislike_evaluation_csv_path, 'r') as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            if row[0] == 'Column1':
                continue

            if EventType(row[Column.EVENT_TYPE]) != EventType.DISLIKE:
                continue

            radek = int(row[Column.RADEK]) if row[Column.RADEK] else None
            ludek = int(row[Column.LUDEK]) if row[Column.LUDEK] else None

            is_session_not_relevant = False

            if radek and radek in {4, 5}:
                is_session_not_relevant = True
            if ludek and ludek in {4, 5}:
                is_session_not_relevant = True
            if ludek and radek and ludek >= 3 and radek >= 3:
                is_session_not_relevant = True

            if is_session_not_relevant:
                not_relevant_sessions.append((row[Column.CLIENT_ID], uuid.UUID(row[Column.CLIENT_ID])))

    return not_relevant_sessions


if __name__ == "__main__":
    not_relevant_sessions = parse_sessions_to_remove(dislike_evaluation_csv_path='csas_logs/dislike_evaluation.csv')
    a = 1
