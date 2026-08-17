"""Pure domain rules, kept separate so they can be tested without Spark."""

from __future__ import annotations

import math


def expected_gear(speed: int | float) -> int:
    """Match Spark's positive-number round(speed / 30) behavior."""

    return int(math.floor((float(speed) / 30.0) + 0.5))


def is_alert(speed: int, rpm: int, gear: int) -> bool:
    """An event is an alert when at least one PDF condition is true."""

    return speed > 120 or expected_gear(speed) != gear or rpm > 6000


def is_silver_family(color_name: str | None) -> bool:
    """Treat gray/grey/silver as the same color, per the project decision."""

    return (color_name or "").strip().lower() in {"gray", "grey", "silver"}

