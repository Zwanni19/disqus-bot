import os
import time
import json
import sqlite3
import requests
import secrets
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from commands.router import dispatch_command
from utils.text import strip_html
from utils.hourly_posts import init_hourly_schedule, tick_hourly_posts

API_BASE = "https://disqus.com/api/3.0"

DISQUS_FORUM_SHORTNAME = os.environ.get("DISQUS_FORUM", "").strip()
DISQUS_PUBLIC_KEY = os.environ.get("DISQUS_PUBLIC_KEY", "").strip()
DISQUS_SECRET_KEY = os.environ.get("DISQUS_SECRET_KEY", "").strip()
DISQUS_ACCESS_TOKEN = os.environ.get("DISQUS_ACCESS_TOKEN", "").strip()

POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "4"))
POST_LIMIT = int(os.environ.get("POST_LIMIT", "50"))

# Thread polling for welcome without user comments
THREAD_POLL_SECONDS = int(os.environ.get("THREAD_POLL_SECONDS", "20"))
THREAD_LIMIT = int(os.environ.get("THREAD_LIMIT", "25"))

# If 1: welcome also for existing threads (not only created after start)
WELCOME_EXISTING = (os.environ.get("WELCOME_EXISTING", "0").strip() == "1")

DEBUG_TRIGGERS = (os.environ.get("DEBUG_TRIGGERS", "0").strip() == "1")

# Moderator cache refresh interval (seconds)
MOD_CACHE_TTL_SECONDS = int(os.environ.get("MOD_CACHE_TTL_SECONDS", "43200"))

# Welcome text (neutral default). You can use "{HEX}" placeholder.
WELCOME_TEXT = (os.environ.get("WELCOME_TEXT", "Hallo #{HEX}") or "Hallo #{HEX}").strip()

if not DISQUS_FORUM_SHORTNAME or not DISQUS_PUBLIC_KEY or not DISQUS_ACCESS_TOKEN:
    raise SystemExit("Missing env vars. Set DISQUS_FORUM, DISQUS_PUBLIC_KEY, DISQUS_ACCESS_TOKEN.")

_BERLIN = ZoneInfo("Europe/Berlin")


def ts() -> str:
    return datetime.now(_BERLIN).isoformat(timespec="milliseconds")


# -------------------------
# DB helpers
# -------------------------
def db_init():
    con = sqlite3.connect("disqus_state.db")
    con.execute("CREATE TABLE IF NOT EXISTS seen_posts (post_id TEXT PRIMARY KEY)")
    con.execute("CREATE TABLE IF NOT EXISTS seen_threads (thread_id TEXT PRIMARY KEY)")
    con.execute("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)")
    con.execute("CREATE TABLE IF NOT EXISTS liked_posts (post_id TEXT PRIMARY KEY)")
    con.execute("CREATE TABLE IF NOT EXISTS pending_unbans (blacklist_id TEXT PRIMARY KEY, due_unix INTEGER NOT NULL)")

    # Ban history (only what THIS bot triggers)
    con.execute("""
        CREATE TABLE IF NOT EXISTS bans_log (
            blacklist_id TEXT PRIMARY KEY,
            thread_id TEXT,
            ban_cmd_post_id TEXT,
            target_post_id TEXT,
            subject_type TEXT,
            subject_label TEXT,
            started_at_unix INTEGER NOT NULL,
            duration_secs INTEGER,
            due_unix INTEGER,
            unbanned_at_unix INTEGER
        )
    """)
    con.commit()
    return con


def ensure_pending_unbans_schema(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS pending_unbans (
            blacklist_id TEXT PRIMARY KEY,
            due_unix INTEGER NOT NULL
        )
    """)
    con.commit()

    cols = [r[1] for r in con.execute("PRAGMA table_info(pending_unbans)").fetchall()]
    if cols == ["blacklist_id", "due_unix"]:
        return

    print(f"{ts()} DB MIGRATION: pending_unbans columns={cols} -> rebuilding")
    con.execute("ALTER TABLE pending_unbans RENAME TO pending_unbans_old")
    con.execute("""
        CREATE TABLE pending_unbans (
            blacklist_id TEXT PRIMARY KEY,
            due_unix INTEGER NOT NULL
        )
    """)

    old_id_col = None
    for c in ["blacklist_id", "id", "block_id", "blacklist"]:
        if c in cols:
            old_id_col = c
            break

    if old_id_col and "due_unix" in cols:
        con.execute(f"""
            INSERT OR IGNORE INTO pending_unbans(blacklist_id, due_unix)
            SELECT CAST({old_id_col} AS TEXT), due_unix FROM pending_unbans_old
        """)

    con.execute("DROP TABLE pending_unbans_old")
    con.commit()


def kv_get(con, k: str):
    cur = con.execute("SELECT v FROM kv WHERE k = ?", (k,))
    row = cur.fetchone()
    return row[0] if row else None


def kv_set(con, k: str, v: str):
    con.execute(
        "INSERT INTO kv(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
        (k, v),
    )
    con.commit()


def seen_post(con, post_id: str) -> bool:
    cur = con.execute("SELECT 1 FROM seen_posts WHERE post_id = ?", (post_id,))
    return cur.fetchone() is not None


def mark_seen_post(con, post_id: str):
    con.execute("INSERT OR IGNORE INTO seen_posts(post_id) VALUES(?)", (post_id,))
    con.commit()


def seen_thread(con, thread_id: str) -> bool:
    cur = con.execute("SELECT 1 FROM seen_threads WHERE thread_id = ?", (thread_id,))
    return cur.fetchone() is not None


def mark_seen_thread(con, thread_id: str):
    con.execute("INSERT OR IGNORE INTO seen_threads(thread_id) VALUES(?)", (thread_id,))
    con.commit()


def liked(con, post_id: str) -> bool:
    cur = con.execute("SELECT 1 FROM liked_posts WHERE post_id = ?", (post_id,))
    return cur.fetchone() is not None


def mark_liked(con, post_id: str):
    con.execute("INSERT OR IGNORE INTO liked_posts(post_id) VALUES(?)", (post_id,))
    con.commit()


# -------------------------
# Disqus API helpers
# -------------------------
def disqus_get(path: str, params: dict):
    p = dict(params or {})
    p.setdefault("api_key", DISQUS_PUBLIC_KEY)
    if DISQUS_ACCESS_TOKEN:
        p.setdefault("access_token", DISQUS_ACCESS_TOKEN)

    r = requests.get(f"{API_BASE}{path}", params=p, timeout=15)
    try:
        data = r.json()
    except Exception:
        r.raise_for_status()
        raise

    if r.status_code >= 400 or data.get("code", 0) != 0:
        raise RuntimeError(f"Disqus API error (HTTP {r.status_code}): {data}")

    return data["response"]


def disqus_post(path: str, data: dict):
    payload = dict(data or {})
    payload.setdefault("api_key", DISQUS_PUBLIC_KEY)

    if DISQUS_SECRET_KEY:
        payload.setdefault("api_secret", DISQUS_SECRET_KEY)
    if DISQUS_ACCESS_TOKEN:
        payload.setdefault("access_token", DISQUS_ACCESS_TOKEN)

    r = requests.post(f"{API_BASE}{path}", data=payload, timeout=20)
    out = r.json()
    if r.status_code >= 400 or out.get("code", 0) != 0:
        raise RuntimeError(f"Disqus API error (HTTP {r.status_code}): {out}")
    return out["response"]


def whoami():
    return disqus_get("/users/details.json", {})


def list_forum_recent_posts(forum_shortname: str, limit: int):
    return disqus_get(
        "/forums/listPosts.json",
        {
            "forum": forum_shortname,
            "limit": int(limit),
            "order": "desc",
            "include": ["approved", "unapproved"],
            "related": ["thread"],
        },
    )


def list_forum_recent_threads(forum_shortname: str, limit: int):
    return disqus_get(
        "/forums/listThreads.json",
        {
            "forum": forum_shortname,
            "limit": int(limit),
            "order": "desc",
        },
    )


def list_forum_moderators(forum_shortname: str, limit: int = 100):
    return disqus_get(
        "/forums/listModerators.json",
        {
            "forum": forum_shortname,
        },
    )


def reply(thread_id: str, parent_post_id: str, message: str):
    return disqus_post("/posts/create.json", {"thread": thread_id, "parent": parent_post_id, "message": message})


def create_root_post(thread_id: str, message: str):
    return disqus_post("/posts/create.json", {"thread": thread_id, "message": message})


def vote_post_like(post_id: str, vote: int = 1):
    return disqus_post("/posts/vote.json", {"post": str(post_id), "vote": str(vote)})


def get_post_details(post_id: str):
    return disqus_get("/posts/details.json", {"post": str(post_id)})


def dbg_trigger(text: str) -> bool:
    if not DEBUG_TRIGGERS:
        return False
    t_low = (text or "").lower()
    return (
        ("bot" in t_low)
        or ("ban" in t_low)
        or ("test" in t_low)
        or ("moin" in t_low)
        or ("hallo" in t_low)
        or ("guten morgen" in t_low)
        or ("mods" in t_low)
        or ("witz" in t_low)
        or ("liebestest" in t_low)
        or ("front" in t_low)
    )


# -------------------------
# Moderator cache
# -------------------------
def _parse_mods(mods_response: list[dict]) -> dict:
    mod_ids = set()
    mod_usernames = set()
    display_names = []

    for item in mods_response or []:
        if not isinstance(item, dict):
            continue
        user = item.get("user") if isinstance(item.get("user"), dict) else None
        if not isinstance(user, dict):
            continue

        uid = str(user.get("id") or "").strip()
        uname = str(user.get("username") or "").strip().lower()
        dname = str(user.get("name") or "").strip()

        if uid:
            mod_ids.add(uid)
        if uname:
            mod_usernames.add(uname)

        # Output requirement: ONLY display names
        if dname:
            display_names.append(dname)

    uniq = []
    seen = set()
    for n in display_names:
        k = n.lower()
        if k in seen:
            continue
        seen.add(k)
        uniq.append(n)

    return {
        "ids": sorted(mod_ids),
        "usernames": sorted(mod_usernames),
        "display_names": uniq,
    }


def refresh_mod_cache_if_needed(con, force: bool = False, log=print):
    now = int(time.time())
    last = int(kv_get(con, "mods_cache_last_unix") or "0")
    if not force and (now - last) < MOD_CACHE_TTL_SECONDS:
        return

    try:
        mods = list_forum_moderators(DISQUS_FORUM_SHORTNAME, limit=100)
        parsed = _parse_mods(mods)
        kv_set(con, "mods_cache_json", json.dumps(parsed, ensure_ascii=False))
        kv_set(con, "mods_cache_last_unix", str(now))
        log(f"{ts()} MOD-CACHE refreshed count_display={len(parsed.get('display_names', []))} ids={len(parsed.get('ids', []))}")
    except Exception as e:
        log(f"{ts()} MOD-CACHE refresh failed: {e}")


def _get_mod_cache(con) -> dict:
    raw = kv_get(con, "mods_cache_json") or ""
    try:
        return json.loads(raw) if raw else {}
    except Exception:
        return {}


def is_moderator(con, author_id: str, author_username: str) -> bool:
    cache = _get_mod_cache(con)
    ids = set(cache.get("ids") or [])
    usernames = set(cache.get("usernames") or [])
    aid = (author_id or "").strip()
    aun = (author_username or "").strip().lower()
    return (aid in ids) or (aun in usernames)


def format_mods_bullets_display_names_only(con) -> str:
    cache = _get_mod_cache(con)
    names = cache.get("display_names") or []
    if not names:
        return "Mods:\n- (keine Anzeigenamen gefunden)"
    lines = ["Mods:"]
    for n in names:
        lines.append(f"- {n}")
    return "\n".join(lines)


# -------------------------
# Bot helpers
# -------------------------
def created_at_to_unix(created_at: str) -> int | None:
    if not created_at:
        return None
    try:
        s = created_at.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None


def get_thread_id_from_post(p: dict) -> str:
    th = p.get("thread")
    if isinstance(th, dict):
        tid = str(th.get("id") or "").strip()
        if tid:
            return tid
    return str(p.get("thread") or "").strip()


def ensure_not_duplicate(con, thread_id: str, message: str) -> str:
    key = f"last_bot_message::{thread_id}"
    last = kv_get(con, key) or ""
    msg = (message or "").rstrip("\n")
    if msg == last:
        msg = msg + " "
    kv_set(con, key, msg)
    return msg


def random_hex6() -> str:
    return secrets.token_hex(3).upper()


def safe_reply(con, thread_id: str, parent_post_id: str, message: str) -> str | None:
    if not thread_id:
        return None
    resp = reply(thread_id, parent_post_id, message)
    new_id = str(resp.get("id") or "").strip()
    return new_id or None


def create_root_post_and_like(con, thread_id: str, message: str, log=print) -> str | None:
    resp = create_root_post(thread_id, message)
    new_id = str(resp.get("id") or "").strip()
    if new_id:
        try:
            if not liked(con, new_id):
                vote_post_like(new_id, vote=1)
                mark_liked(con, new_id)
                log(f"{ts()} SELF-LIKED root_post_id={new_id}")
        except Exception as e:
            log(f"{ts()} SELF-LIKE failed root_post_id={new_id}: {e}")
        return new_id
    return None


def like_own_post_if_needed(con, post_id: str, log=print):
    if not post_id:
        return
    if liked(con, post_id):
        return
    try:
        vote_post_like(post_id, vote=1)
        mark_liked(con, post_id)
        log(f"{ts()} SELF-LIKED post_id={post_id}")
    except Exception as e:
        log(f"{ts()} SELF-LIKE failed post_id={post_id}: {e}")


def should_like(text: str) -> bool:
    return "like mal" in ((text or "").lower())


def is_own_post(p: dict, me_id: str, me_username: str) -> bool:
    a = p.get("author") or {}
    aid = str(a.get("id") or "").strip()
    aun = str(a.get("username") or "").strip().lower()
    if me_id and aid and aid == str(me_id):
        return True
    if me_username and aun and aun == str(me_username).lower():
        return True
    return False


# -------------------------
# BAN workaround
# -------------------------
def ban_post_author_permanent(
    post_id: str,
    *,
    ban_user=True,
    ban_email=False,
    ban_ip=False,
    shadow_ban=False,
    retroactive_action=None,
):
    data = {
        "post": str(post_id),
        "banUser": "1" if ban_user else "0",
        "banEmail": "1" if ban_email else "0",
        "banIp": "1" if ban_ip else "0",
        "shadowBan": "1" if shadow_ban else "0",
    }
    if retroactive_action is not None:
        data["retroactiveAction"] = str(int(retroactive_action))
    return disqus_post("/forums/block/banPostAuthor.json", data)


def blacklist_remove_by_id(blacklist_id: str):
    return disqus_post("/blacklists/remove.json", {"forum": DISQUS_FORUM_SHORTNAME, "blacklist": [str(blacklist_id)]})


def schedule_unban(con, blacklist_id: str, due_unix: int):
    con.execute(
        "INSERT OR REPLACE INTO pending_unbans(blacklist_id, due_unix) VALUES(?, ?)",
        (str(blacklist_id), int(due_unix)),
    )
    con.commit()


def tick_unbans(con, log=print):
    now = int(time.time())
    rows = con.execute(
        "SELECT blacklist_id, due_unix FROM pending_unbans WHERE due_unix <= ? ORDER BY due_unix ASC LIMIT 50",
        (now,),
    ).fetchall()

    for blacklist_id, due_unix in rows:
        try:
            blacklist_remove_by_id(str(blacklist_id))
            mark_unbanned_in_log(con, str(blacklist_id), now)
            log(f"{ts()} UNBANNED blacklist_id={blacklist_id}")
            con.execute("DELETE FROM pending_unbans WHERE blacklist_id = ?", (str(blacklist_id),))
            con.commit()
        except Exception as e:
            log(f"{ts()} UNBAN failed blacklist_id={blacklist_id}: {e}")


# -------------------------
# Ban log + report
# -------------------------
def _fmt_secs(secs: int) -> str:
    secs = int(secs)
    if secs < 0:
        secs = 0
    d, rem = divmod(secs, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    if d:
        return f"{d}d{h:02}h{m:02}m{s:02}s"
    if h:
        return f"{h}h{m:02}m{s:02}s"
    if m:
        return f"{m}m{s:02}s"
    return f"{s}s"


def extract_ban_subjects_user_only(resp: dict) -> list[dict]:
    out = []
    updated = (resp or {}).get("updated") or []
    for item in updated:
        if not isinstance(item, dict):
            continue
        stype = (item.get("type") or "").strip().lower()
        if stype != "user":
            continue
        bid = item.get("id")
        val = item.get("value")
        if bid is None or not isinstance(val, dict):
            continue

        name = (val.get("name") or "").strip()
        username = (val.get("username") or "").strip()
        label = name or (f"@{username}" if username else "user")

        out.append(
            {
                "blacklist_id": str(bid),
                "subject_type": "user",
                "subject_label": label,
                "subject_username": username.strip().lower() if username else "",
            }
        )
    return out


def log_ban_event(
    con,
    *,
    blacklist_id: str,
    thread_id: str,
    ban_cmd_post_id: str,
    target_post_id: str,
    subject_type: str,
    subject_label: str,
    started_at_unix: int,
    duration_secs: int | None,
    due_unix: int | None,
):
    con.execute(
        """
        INSERT OR REPLACE INTO bans_log
        (blacklist_id, thread_id, ban_cmd_post_id, target_post_id, subject_type, subject_label,
         started_at_unix, duration_secs, due_unix, unbanned_at_unix)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT unbanned_at_unix FROM bans_log WHERE blacklist_id=?), NULL))
        """,
        (
            str(blacklist_id),
            str(thread_id),
            str(ban_cmd_post_id),
            str(target_post_id),
            str(subject_type),
            str(subject_label),
            int(started_at_unix),
            int(duration_secs) if duration_secs else None,
            int(due_unix) if due_unix else None,
            str(blacklist_id),
        ),
    )
    con.commit()


def mark_unbanned_in_log(con, blacklist_id: str, unbanned_at_unix: int):
    con.execute(
        "UPDATE bans_log SET unbanned_at_unix=? WHERE blacklist_id=?",
        (int(unbanned_at_unix), str(blacklist_id)),
    )
    con.commit()


def build_ban_report_last24h(con, now_unix: int, limit: int = 15) -> str:
    since = int(now_unix) - 86400
    rows = con.execute(
        """
        SELECT subject_label, started_at_unix, duration_secs, due_unix, unbanned_at_unix
        FROM bans_log
        WHERE started_at_unix >= ?
          AND subject_type = 'user'
        ORDER BY started_at_unix DESC
        """,
        (since,),
    ).fetchall()

    if not rows:
        return "Bans letzte 24h (Bot-Log):\n- (keine)"

    lines = ["Bans letzte 24h (Bot-Log):"]
    shown = 0

    for (label, started, dur, due, unbanned) in rows:
        if shown >= limit:
            break

        started = int(started)
        if dur is None and due is None:
            if unbanned:
                banned_for = int(unbanned) - started
                line = f"- {label} — WAR gebannt {_fmt_secs(banned_for)}"
            else:
                line = f"- {label} — AKTIV PERM (seit {_fmt_secs(now_unix - started)})"
        else:
            if unbanned:
                banned_for = int(unbanned) - started
                line = f"- {label} — WAR gebannt {_fmt_secs(banned_for)}"
            else:
                if due and now_unix < int(due):
                    rest = int(due) - now_unix
                    line = f"- {label} — AKTIV REST {_fmt_secs(rest)} (seit {_fmt_secs(now_unix - started)})"
                elif due and now_unix >= int(due):
                    overdue = now_unix - int(due)
                    line = f"- {label} — FÄLLIG seit {_fmt_secs(overdue)}"

        lines.append(line)
        shown += 1

    return "\n".join(lines)


def post_ban_report(con, thread_id: str, now_unix: int, log=print):
    report = build_ban_report_last24h(con, now_unix=now_unix, limit=15)
    msg = ensure_not_duplicate(con, thread_id, report)
    try:
        create_root_post_and_like(con, thread_id, msg, log=log)
        log(f"{ts()} BAN-REPORT posted thread_id={thread_id}")
    except Exception as e:
        log(f"{ts()} BAN-REPORT failed thread_id={thread_id}: {e}")


# -------------------------
# NEW THREAD WELCOME
# -------------------------
def tick_new_threads_and_welcome(con, start_unix: int, log=print):
    now_unix = int(time.time())
    last_poll = int(kv_get(con, "last_thread_poll_unix") or "0")
    if now_unix - last_poll < THREAD_POLL_SECONDS:
        return

    kv_set(con, "last_thread_poll_unix", str(now_unix))

    try:
        threads = list_forum_recent_threads(DISQUS_FORUM_SHORTNAME, THREAD_LIMIT)
    except Exception as e:
        log(f"{ts()} THREADS poll error: {e}")
        return

    new_count = 0

    for th in reversed(threads):
        thread_id = str(th.get("id") or "").strip()
        if not thread_id:
            continue

        if seen_thread(con, thread_id):
            continue

        created_u = created_at_to_unix(th.get("createdAt"))
        if not WELCOME_EXISTING:
            if created_u is not None and created_u < start_unix:
                mark_seen_thread(con, thread_id)
                continue

        if th.get("isClosed") is True:
            mark_seen_thread(con, thread_id)
            continue

        welcome_key = f"welcomed::{thread_id}"
        if kv_get(con, welcome_key) == "1":
            mark_seen_thread(con, thread_id)
            continue

        welcome_text = WELCOME_TEXT.replace("{HEX}", random_hex6())
        welcome_msg = ensure_not_duplicate(con, thread_id, welcome_text)

        try:
            create_root_post_and_like(con, thread_id, welcome_msg, log=log)
            kv_set(con, welcome_key, "1")
            mark_seen_thread(con, thread_id)
            new_count += 1
            log(f"{ts()} WELCOME posted thread_id={thread_id}")
            time.sleep(0.2)
        except Exception as e:
            s = str(e).lower()
            if "thread" in s and "closed" in s:
                kv_set(con, welcome_key, "1")
                mark_seen_thread(con, thread_id)
            else:
                log(f"{ts()} WELCOME error thread_id={thread_id}: {e}")

    if new_count:
        log(f"{ts()} THREADS scanned={len(threads)} new_welcomes={new_count}")


# -------------------------
# MAIN
# -------------------------
def main():
    con = db_init()
    ensure_pending_unbans_schema(con)

    print(f"{ts()} ForumShortname={DISQUS_FORUM_SHORTNAME} | Poll={POLL_SECONDS}s | Limit={POST_LIMIT}")
    print(f"{ts()} ThreadPoll={THREAD_POLL_SECONDS}s | ThreadLimit={THREAD_LIMIT} | WelcomeExisting={WELCOME_EXISTING}")
    print(f"{ts()} Bot running. Ctrl+C to stop.")

    me = whoami()
    me_id = str(me.get("id") or "").strip()
    me_username = str(me.get("username") or "").strip()
    print(f"{ts()} AUTH user={me_username} id={me_id}")

    start_unix = int(datetime.now(timezone.utc).timestamp())
    kv_set(con, "start_unix", str(start_unix))

    refresh_mod_cache_if_needed(con, force=True, log=print)

    next_hourly_post_unix = init_hourly_schedule(con, kv_get, kv_set, log=print)

    try:
        while True:
            refresh_mod_cache_if_needed(con, force=False, log=print)

            tick_new_threads_and_welcome(con, start_unix, log=print)

            try:
                posts = list_forum_recent_posts(DISQUS_FORUM_SHORTNAME, POST_LIMIT)

                for p in reversed(posts):
                    post_id = str(p.get("id", "")).strip()
                    if not post_id or seen_post(con, post_id):
                        continue

                    created_u = created_at_to_unix(p.get("createdAt"))
                    if created_u is not None and created_u < start_unix:
                        mark_seen_post(con, post_id)
                        continue

                    mark_seen_post(con, post_id)

                    if p.get("isSpam") or p.get("isDeleted"):
                        continue

                    thread_id = get_thread_id_from_post(p)
                    if not thread_id:
                        continue

                    kv_set(con, "last_seen_thread_id", thread_id)

                    if is_own_post(p, me_id=me_id, me_username=me_username):
                        like_own_post_if_needed(con, post_id, log=print)
                        continue

                    raw = p.get("message", "") or ""
                    text = strip_html(raw).replace("\u00a0", " ")
                    text = " ".join(text.split())

                    if dbg_trigger(text):
                        print(f"{ts()} SEEN post_id={post_id} thread_id={thread_id} text={text!r}")

                    response = dispatch_command(text)

                    if dbg_trigger(text):
                        print(f"{ts()} DISPATCH post_id={post_id} -> {response!r}")

                    # MODS marker
                    if response == "__MODS__":
                        msg = format_mods_bullets_display_names_only(con)
                        safe_msg = ensure_not_duplicate(con, thread_id, msg)
                        bot_post_id = safe_reply(con, thread_id, post_id, safe_msg)
                        if bot_post_id:
                            like_own_post_if_needed(con, bot_post_id, log=print)
                        continue

                    # BAN marker
                    if response and response.startswith("__BAN__:"):
                        raw_arg = response.split(":", 1)[1].strip()

                        secs = 0
                        is_perm = False

                        if raw_arg.upper() == "PERM":
                            is_perm = True
                        else:
                            try:
                                secs = int(raw_arg)
                                if secs <= 0:
                                    secs = 0
                            except Exception:
                                secs = 0

                        author = p.get("author") or {}
                        author_username = (author.get("username") or "").strip().lower()
                        author_id = str(author.get("id") or "").strip()

                        if not is_moderator(con, author_id=author_id, author_username=author_username):
                            print(f"{ts()} BAN ignored: author is not a forum moderator (id={author_id} username={author_username})")
                            continue

                        target_post_id = str(p.get("parent") or "").strip()
                        if not target_post_id:
                            print(f"{ts()} BAN ignored: no parent post found (reply 'ban' to target comment).")
                            continue

                        try:
                            target_post = get_post_details(target_post_id)
                        except Exception as e:
                            print(f"{ts()} BAN ignored: cannot fetch target post {target_post_id}: {e}")
                            continue

                        target_author = (target_post or {}).get("author") or {}
                        target_author_username = (target_author.get("username") or "").strip().lower()
                        target_author_id = str(target_author.get("id") or "").strip()

                        if (me_id and target_author_id == me_id) or (me_username and target_author_username == me_username.lower()):
                            print(f"{ts()} BAN ignored: target is bot itself")
                            continue

                        if is_moderator(con, author_id=target_author_id, author_username=target_author_username):
                            print(f"{ts()} BAN ignored: target is a forum moderator")
                            continue

                        last_ban_target = (kv_get(con, "last_ban_target_post_id") or "").strip()
                        last_ban_ts = int(kv_get(con, "last_ban_unix") or "0")
                        now_unix = int(time.time())
                        if last_ban_target == target_post_id and (now_unix - last_ban_ts) < 60:
                            print(f"{ts()} BAN ignored: duplicate target within 60s target_post_id={target_post_id}")
                            continue

                        try:
                            started = int(time.time())

                            resp = ban_post_author_permanent(
                                target_post_id,
                                ban_user=True,
                                ban_email=False,
                                ban_ip=False,
                                shadow_ban=False,
                            )

                            subjects = extract_ban_subjects_user_only(resp)
                            if not subjects:
                                print(f"{ts()} BAN ignored: no user blacklist entry returned")
                                continue

                            due = (started + secs) if (secs > 0 and not is_perm) else None

                            if secs > 0 and not is_perm and due is not None:
                                for s in subjects:
                                    schedule_unban(con, s["blacklist_id"], int(due))

                            for s in subjects:
                                log_ban_event(
                                    con,
                                    blacklist_id=s["blacklist_id"],
                                    thread_id=thread_id,
                                    ban_cmd_post_id=post_id,
                                    target_post_id=target_post_id,
                                    subject_type="user",
                                    subject_label=s["subject_label"],
                                    started_at_unix=started,
                                    duration_secs=(secs if (secs > 0 and not is_perm) else None),
                                    due_unix=(int(due) if due else None),
                                )

                            confirm_txt = "OK."
                            confirm = ensure_not_duplicate(con, thread_id, confirm_txt)
                            bot_post_id = safe_reply(con, thread_id, post_id, confirm)
                            if bot_post_id:
                                like_own_post_if_needed(con, bot_post_id, log=print)

                            post_ban_report(con, thread_id=thread_id, now_unix=int(time.time()), log=print)

                            kv_set(con, "last_ban_target_post_id", target_post_id)
                            kv_set(con, "last_ban_unix", str(int(time.time())))

                            print(f"{ts()} BAN done target_post_id={target_post_id} secs={'PERM' if (is_perm or secs == 0) else secs}")

                        except Exception as e:
                            print(f"{ts()} BAN failed target_post_id={target_post_id}: {e}")

                        continue

                    # normal reply
                    bot_post_id = None
                    did_reply = False

                    if response:
                        safe_msg = ensure_not_duplicate(con, thread_id, response)
                        bot_post_id = safe_reply(con, thread_id, post_id, safe_msg)
                        if bot_post_id:
                            did_reply = True
                            like_own_post_if_needed(con, bot_post_id, log=print)
                            print(f"{ts()} Replied post_id={post_id} (bot_post_id={bot_post_id})")
                            time.sleep(0.2)

                    if (did_reply or should_like(text)) and not liked(con, post_id):
                        try:
                            vote_post_like(post_id, vote=1)
                            mark_liked(con, post_id)
                            print(f"{ts()} Liked parent post_id={post_id}")
                        except Exception as e:
                            print(f"{ts()} Like failed parent post_id={post_id}: {e}")

            except Exception as e:
                print(f"{ts()} Error: {e}")
                time.sleep(5)

            next_hourly_post_unix = tick_hourly_posts(
                con=con,
                next_hourly_post_unix=next_hourly_post_unix,
                kv_set=kv_set,
                get_default_thread_id=lambda _con: (kv_get(_con, "last_seen_thread_id") or "").strip() or None,
                ensure_not_duplicate=ensure_not_duplicate,
                create_root_post=lambda thread_id, msg: create_root_post_and_like(con, thread_id, msg, log=print),
                log=print,
            )

            tick_unbans(con, log=print)

            time.sleep(POLL_SECONDS)

    except KeyboardInterrupt:
        print(f"{ts()} Stopping...")
        return


if __name__ == "__main__":
    main()
