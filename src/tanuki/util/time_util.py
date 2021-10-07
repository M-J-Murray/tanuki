from datetime import datetime
from typing import Optional
import re

from dateutil.relativedelta import relativedelta
from dateutil.parser import parse

def parse_delta(str_delta: str) -> Optional[relativedelta]:
    pattern = re.compile(r"^\s*(\d+)(wk|ms|mo|s|m|h|d|)\s*?")
    matches = pattern.match(str_delta)
    if matches is None:
        return None

    amount = int(matches.group(1))
    interval = matches.group(2)
    return {
        "wk": relativedelta(weeks=amount),
        "ms": relativedelta(microseconds=amount),
        "mo": relativedelta(months=amount),
        "s": relativedelta(seconds=amount),
        "m": relativedelta(minutes=amount),
        "h": relativedelta(hours=amount),
        "d": relativedelta(days=amount),
    }[interval]
    


def format_delta(delta: relativedelta) -> Optional[str]:
    if delta.months > 0:
        return f"{delta.months}mo"
    elif delta.weeks > 0:
        return f"{delta.weeks}wk"
    elif delta.days > 0:
        return f"{delta.days}d"
    elif delta.hours > 0:
        return f"{delta.hours}h"
    elif delta.minutes > 0:
        return f"{delta.minutes}m"
    elif delta.days > 0:
        return f"{delta.days}d"
    elif delta.microseconds > 0:
        return f"{delta.microseconds}ms"
    else:
        return None

parse_delta("1h")