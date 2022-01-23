#!/usr/bin/env python3
"""Prototype for event classification of streaming sensor data."""

# Open issues
# * Bugfix: Handle old matches in sliding window to avoid re-matching old
#           hits.
# * Optimize: Select rules that could match a stream and discard the
#             other rules for that stream.
# * Feature: Generalize stream abstraction and rule handling to permit arbitrary
#            levels of rules and streams and rule applications.
# * Feature: Let different rules be able to apply to different wize of sliding
#            window. Higher abstractions may need bigger window to emit matches.

from random import choices
from typing import Callable, List, Optional, Tuple

Event = str


def mk_sensor_stream(chunksize: int) -> Callable:
    """Return a sensor stream generator.

    chunksize: Number of events in each sensor stream generator call."""

    enclosing_scope_possible_events: List[Event] = [
        f"Sensor_Event_{evt:04d}" for evt in range(1, 101)
    ]
    enclosing_scope_chunksize: int = chunksize

    def sensor_stream_chunk() -> List[Event]:
        """Generate and return next sensor event stream chunk."""

        return choices(
            population=enclosing_scope_possible_events, k=enclosing_scope_chunksize
        )

    return sensor_stream_chunk


def move_window():
    """TBD"""


def rule_1_sensor_stream_1(chunk: List[Event]) -> Optional[Tuple[Event, List[Event]]]:
    """Rule 1 for sensor stream 1."""

    if "Sensor_Event_0026" in chunk:
        return "Abstracted_Level1_Event_0001", ["Sensor_Event_0026"]
    return None


def rule_2_sensor_stream_1(chunk: List[Event]) -> Optional[Tuple[Event, List[Event]]]:
    """Rule 2 for sensor stream 1."""

    if (
        "Sensor_Event_0028" in chunk
        and "Sensor_Event_0002" in chunk
        and "Sensor_Event_0099" not in chunk
    ):
        return "Abstracted_Level1_Event_0002", [
            "Sensor_Event_0028",
            "Sensor_Event_0002",
            "!Sensor_Event_0099",
        ]
    return None


def rule_1_abstracted_stream_1(
    chunk: List[Event],
) -> Optional[Tuple[Event, List[Event]]]:
    """Rule 1 for abstracted stream 1."""

    if (
        "Abstracted_Level1_Event_0001" in chunk
        and "Abstracted_Level1_Event_0002" in chunk
    ):
        return "Abstracted_Level2_Event_0001", [
            "Abstracted_Level1_Event_0001",
            "Abstracted_Level1_Event_0002",
        ]
    return None


def abstraction_rules() -> List[Callable]:
    """Set up and return list of abstraction rules."""

    rules: List[Callable] = []
    rules.append(rule_1_sensor_stream_1)
    rules.append(rule_2_sensor_stream_1)
    rules.append(rule_1_abstracted_stream_1)

    return rules


def elevate_abstraction(
    chunk: List[Event], rules: List[Callable]
) -> List[Tuple[Event, List[Event]]]:
    """Given events and abstraction rules, return list of higher abstractions and
    corresponding triggering lower level events."""

    matches = [hit for hit in [rule(chunk) for rule in rules] if hit is not None]

    return matches


def main():
    """Entry point."""
    sensor_stream = mk_sensor_stream(50)
    rules = abstraction_rules()

    for chunk_counter in range(0, 40):
        print(f"Chunk {chunk_counter}")
        sensor_stream_chunk = sensor_stream()
        abstracted_stream_level1: List[Event] = []
        for abstracted_event, lower_events in elevate_abstraction(
            sensor_stream_chunk, rules
        ):
            abstracted_stream_level1.append(
                abstracted_event
            )  # daos: Add found event to new stream.
            print(f" Lvl 0->1 => {abstracted_event} => ({lower_events})")

        # daos: Check rules against new abstracted stream.
        for abstracted_event, lower_events in elevate_abstraction(
            abstracted_stream_level1, rules
        ):
            print(f" Lvl 1->2 => {abstracted_event} => ({lower_events})")


if __name__ == "__main__":
    main()
