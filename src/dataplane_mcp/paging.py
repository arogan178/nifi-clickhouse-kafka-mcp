import re
from datetime import UTC, datetime, timedelta

RELATIVE_TIME = re.compile(r"-(\d+)([smhd])")
UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}


def page(items: list, offset: int, limit: int) -> dict:
    offset, limit = max(0, offset), max(1, limit)
    end = offset + limit
    return {
        "total": len(items),
        "offset": offset,
        "limit": limit,
        "next_offset": end if end < len(items) else None,
        "items": items[offset:end],
    }


def to_millis(spec: str, now: datetime | None = None) -> int:
    """An ISO timestamp, or "-30m"/"-1h"/"-2d" meaning that long before now, as epoch milliseconds."""
    now = now or datetime.now(UTC)
    relative = RELATIVE_TIME.fullmatch(spec)
    if relative:
        moment = now - timedelta(**{UNITS[relative[2]]: int(relative[1])})
    else:
        moment = datetime.fromisoformat(spec)
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=UTC)
    return int(moment.timestamp() * 1000)
