import os
import random
from datetime import datetime, timezone


def _now_unix() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def schedule_next_hourly_post(now_unix: int) -> int:
    """
    Pick a random second within the next hour window.
    Example: now=10:12 -> next scheduled somewhere between 11:00:00 and 11:59:59 UTC.
    """
    hour_start = (now_unix // 3600) * 3600
    next_hour_start = hour_start + 3600
    return next_hour_start + random.randint(0, 3599)


def load_hourly_messages() -> list[str]:
    """
    Load messages from env var HOURLY_MESSAGES separated by '|'.
    If not set, use defaults.
    Example:
      $env:HOURLY_MESSAGES = "Hallo|Noch wer da?|Moin"
    """
    raw = (os.environ.get("HOURLY_MESSAGES") or "").strip()
    if raw:
        msgs = [m.strip() for m in raw.split("|") if m.strip()]
        if msgs:
            return msgs

    # Default pool (neutral; edit as you like)
    return [
        "Hallo",
        "Noch wer da?",
        "Moin",
        "Ping.",
        "Was geht?",
        "Na, Ihr Hurensöhne?!",
        "Jetzt leg mal eine Dadash!",
        "s/o an AnisFencheltee, mein Herr und Gebieter!",
        "12. Februar ist 31GG Feiertag!",
    ]


def init_hourly_schedule(con, kv_get, kv_set, log=print) -> int:
    """
    Initialize next_hourly_post_unix from DB or schedule new one.
    Returns next_hourly_post_unix (int).
    """
    stored = kv_get(con, "next_hourly_post_unix")
    if stored and str(stored).isdigit():
        next_unix = int(stored)
        """log(f"Next hourly post loaded unix={next_unix}")"""
        return next_unix

    now = _now_unix()
    next_unix = schedule_next_hourly_post(now)
    kv_set(con, "next_hourly_post_unix", str(next_unix))
    log(f"Next hourly post scheduled unix={next_unix}")
    return next_unix


def tick_hourly_posts(
    con,
    next_hourly_post_unix: int,
    kv_set,
    get_default_thread_id,
    ensure_not_duplicate,
    create_root_post,
    log=print,
) -> int:
    """
    Call this once per main loop.
    If due -> post random message to default thread and reschedule.
    Returns updated next_hourly_post_unix.
    """
    now = _now_unix()
    if now < next_hourly_post_unix:
        return next_hourly_post_unix

    thread_id = get_default_thread_id(con)
    messages = load_hourly_messages()

    if thread_id and messages:
        msg = random.choice(messages)
        msg = ensure_not_duplicate(con, thread_id, msg)

        try:
            create_root_post(thread_id, msg)
            log(f"Hourly post sent in thread_id={thread_id}")
        except Exception as e:
            s = str(e).lower()
            if "thread" in s and "closed" in s:
                log(f"Hourly post skipped (thread closed) thread_id={thread_id}")
            else:
                log(f"Hourly post error: {e}")
    else:
        log("Hourly post skipped (no thread_id yet).")

    # Always reschedule for next hour window
    now = _now_unix()
    next_hourly_post_unix = schedule_next_hourly_post(now)
    kv_set(con, "next_hourly_post_unix", str(next_hourly_post_unix))
    log(f"Next hourly post scheduled unix={next_hourly_post_unix}")
    return next_hourly_post_unix
