from datetime import datetime, timedelta
from math import floor
import re


def getf(matcher: re.Match, group: int, default: float) -> float:
    value = matcher.group(group)
    return float(value) if value is not None else default


def geti(matcher: re.Match, group: int, default: int) -> int:
    value = matcher.group(group)
    return int(value) if value is not None else default


def parse_datetime(str_datetime: str) -> datetime:
    pattern = re.compile(
        r"^(\d{2})-(\d{2})-(\d{2,4})(?:T(\d{2})(?:\:(\d{2})(?:\:(\d{2}))?)?)?$"
    )
    matches = pattern.match(str_datetime)
    if bool(matches):
        return datetime(
            day=geti(matches, 1, 0),
            month=geti(matches, 2, 0),
            year=geti(matches, 3, 0),
            hour=geti(matches, 4, 0),
            minute=geti(matches, 5, 0),
            second=geti(matches, 6, 0),
        )
    else:
        raise ValueError("Invalid timestamp string")


def format_datetime(timestamp: datetime) -> str:
    str_def = f"{timestamp.day}-{timestamp.month}-{timestamp.year}"
    if timestamp.second > 0:
        str_def += f"T{timestamp.hour}:{timestamp.minute}:{timestamp.second}"
    elif timestamp.minute > 0:
        str_def += f"T{timestamp.hour}:{timestamp.minute}"
    elif timestamp.hour > 0:
        str_def += f"T{timestamp.hour}"
    return str_def


def parse_delta(str_delta: str) -> timedelta:
    pattern = re.compile(
        r"^(?:(\d+)d\s*)?"
        + r"(?:(\d+)h\s*)?"
        + r"(?:(\d+)m\s*)?"
        + r"(?:(\d+)s\s*)?"
        + r"(?:(\d+)ms\s*)?"
        + r"(?:(\d+)us\s*)?$"
    )
    matches = pattern.match(str_delta)
    if bool(matches):
        return timedelta(
            seconds=(
                (getf(matches, 1, 0) * (24 * 60 * 60))
                + (getf(matches, 2, 0) * (60 * 60))
                + (getf(matches, 3, 0) * 60)
                + (getf(matches, 4, 0))
            ),
            microseconds=((getf(matches, 5, 0) * 1e3) + (getf(matches, 6, 0))),
        )
    else:
        raise ValueError("Invalid timedelta string")


def format_delta(delta: timedelta) -> str:
    units = []
    units.append(delta.total_seconds() / (60 * 60 * 24))
    units.append((units[-1] % 1) * 24)
    units.append((units[-1] % 1) * 60)
    units.append((units[-1] % 1) * 60)
    units.append(floor(delta.microseconds * 1e-3))
    units.append(delta.microseconds - (units[-1] * 1e3))

    units = [floor(unit) for unit in units]
    symbols = ["d", "h", "m", "s", "ms", "us"]
    return " ".join(
        [f"{unit}{symbol}" for unit, symbol in zip(units, symbols) if unit > 0]
    )
