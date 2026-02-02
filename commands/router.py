import re
import random
import requests

from commands.weather import handle_weather
from commands.story_31gg import handle_story_31gg
from commands.size import handle_size
from commands.opinion import handle_opinion, handle_explain
from commands.liebestest import handle_liebestest


def _normalize(text: str) -> str:
    if not text:
        return ""
    t = text.replace("\u00a0", " ")
    t = " ".join(t.split()).strip().lower()
    t = (
        t.replace("ä", "ae")
         .replace("ö", "oe")
         .replace("ü", "ue")
         .replace("ß", "ss")
    )
    return t


def _strip_trailing_punct(s: str) -> str:
    # removes trailing punctuation like ".", ",", "!", "?" etc.
    return re.sub(r"[^\w\-]+$", "", (s or "").strip())


# --- generic fronts for "bot sag front" (no args) ---
GENERIC_FRONTS = [
    "Du Hase.",
    "Heute nicht, mein Freund.",
    "Mach langsam, Chef.",
    "Ganz dünnes Eis.",
    "Stark. Wirklich stark.",
]

# --- targeted fronts for "bot sag front an|zu|gegen <user>" ---
# Output must NOT include an|zu|gegen, only the name.
TARGETED_FRONTS = [
    "{name} du Hase",
    "{name} entspann dich mal",
    "{name} das war’s jetzt aber",
    "{name} heute bist du aber mutig",
    "{name} ganz dünnes Eis",
]


# --- general triggers without "bot" prefix ---
P_TEST = re.compile(r"^test\b.*$", re.IGNORECASE)
P_GREET = re.compile(r"^(moin|hallo|guten\s+morgen|hey)\b.*$", re.IGNORECASE)

# --- bot commands ---
P_HELP_1 = re.compile(r"^bot\s+sag\s+befehle\b.*$", re.IGNORECASE)
P_HELP_2 = re.compile(r"^bot\s+hilfe\b.*$", re.IGNORECASE)

P_MODS = re.compile(r"^bot\s+sag\s+mods\b.*$", re.IGNORECASE)

P_WEATHER = re.compile(r"^bot\s+sag\s+wetter(?:\s+in)?\s+(.+)$", re.IGNORECASE)

# "bot sag front" OR "bot sag front an|zu|gegen <user>"
P_FRONT = re.compile(
    r"^bot\s+sag\s+front(?:\s+(an|zu|gegen))?(?:\s+(\S+))?(?:\s+.*)?$",
    re.IGNORECASE
)

P_STORY_31GG = re.compile(
    r"^bot\s+erzaehl(?:e)?\s+mir\s+die\s+geschichte\s+von\s+31gg\s*$",
    re.IGNORECASE
)

P_SIZE = re.compile(r"^bot\s+sag\s+schwanzl(?:aenge|ange)?\b.*$", re.IGNORECASE)

P_JOKE_1 = re.compile(r"^bot\s+sag\s+witz\b.*$", re.IGNORECASE)
P_JOKE_2 = re.compile(r"^bot\s+erzaehl(?:e)?(?:\s+mir)?(?:\s+einen)?\s+witz\b.*$", re.IGNORECASE)

# liebestest: expects 2 args
P_LIEBESTEST = re.compile(r"^bot\s+sag\s+liebestest\b\s*(.*)$", re.IGNORECASE)

# BAN: "ban" or "ban 5m" or "ban 1h" etc.
P_BAN = re.compile(r"\bban(?:\s+(\d+)\s*([smhd]))?\b", re.IGNORECASE)

P_LLM = re.compile(
    r"^bot\s+(?:"
    r"(?:sag\s+)?erklaer(?:e)?|"
    r"(?:sag\s+)?meinung\s+zu|"
    r"was\s+sind|was\s+ist"
    r")\s+(.+)$",
    re.IGNORECASE
)

P_SAG_ANY = re.compile(r"^bot\s+sag\s+(.+)$", re.IGNORECASE)


def _help_text() -> str:
    return (
        "Aktive Befehle:\n"
        "- bot sag mods\n"
        "- bot sag wetter [in] <stadt>\n"
        "- bot sag front\n"
        "- bot sag front an|zu|gegen <user>\n"
        "- bot erzähl(e) mir die geschichte von 31gg\n"
        "- bot sag schwanzlänge ...\n"
        "- bot sag witz / bot erzähl(e) (einen) witz\n"
        "- bot sag liebestest <UserA> <UserB>\n"
        "- bot hilfe / bot sag befehle\n"
        "- bot sag meinung zu <thema>\n"
        "- bot erklär(e) <thema> / bot was ist|was sind <thema>\n"
        "- ban 5m|1h|2d oder nur 'ban' (PERM) als Reply auf Zielkommentar (nur Moderatoren)\n"
        "\n"
        "Auto-Triggers:\n"
        "- test -> bestanden.\n"
        "- moin/hallo/guten morgen/hey -> moin\n"
    )


def _fetch_random_joke_de() -> str:
    try:
        url = "https://v2.jokeapi.dev/joke/Any"
        params = {
            "lang": "de",
            "type": "single",
            "blacklistFlags": "nsfw,religious,political,racist,sexist,explicit",
        }
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        if data.get("error"):
            return "Kein Witz gefunden."
        joke = (data.get("joke") or "").strip()
        return joke or "Kein Witz gefunden."
    except Exception:
        return "Kein Witz gefunden."


def dispatch_command(text: str) -> str | None:
    t = _normalize(text)
    if not t:
        return None

    # general triggers first
    if P_TEST.match(t):
        return "bestanden."
    if P_GREET.match(t):
        return "moin"

    # help
    if P_HELP_1.match(t) or P_HELP_2.match(t):
        return _help_text()

    # mods list -> handled in bot.py (Forum/listModerators)
    if P_MODS.match(t):
        return "__MODS__"

    # BAN marker (handled in bot.py)
    m = P_BAN.search(t)
    if m:
        num = m.group(1)
        unit = m.group(2)

        if not num or not unit:
            return "__BAN__:PERM"

        try:
            n = int(num)
        except Exception:
            return "__BAN__:PERM"

        unit = unit.lower()
        mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}.get(unit)
        if not mult or n <= 0:
            return "__BAN__:PERM"

        return f"__BAN__:{n * mult}"

    # jokes
    if P_JOKE_1.match(t) or P_JOKE_2.match(t):
        return _fetch_random_joke_de()

    # weather
    m = P_WEATHER.match(t)
    if m:
        return handle_weather(m.group(1).strip())

    # front (generic or targeted)
    m = P_FRONT.match(t)
    if m:
        mode = (m.group(1) or "").strip().lower()   # an|zu|gegen or ""
        target = (m.group(2) or "").strip()         # username or ""

        # Case 1: "bot sag front" -> generic front
        if not mode and not target:
            return random.choice(GENERIC_FRONTS)

        # Case 2: "bot sag front an|zu|gegen <user>" -> targeted front (ignore mode in output)
        if mode and target:
            name = _strip_trailing_punct(target).lstrip("@")
            if not name:
                return "Usage: bot sag front an|zu|gegen <user> oder nur: bot sag front"
            tpl = random.choice(TARGETED_FRONTS)
            return tpl.format(name=name)

        return "Usage: bot sag front an|zu|gegen <user> oder nur: bot sag front"

    # story
    if P_STORY_31GG.match(t):
        return handle_story_31gg()

    # size
    if P_SIZE.match(t):
        return handle_size()

    # liebestest (2 args)
    m = P_LIEBESTEST.match(t)
    if m:
        payload = (m.group(1) or "").strip()

        # allow separators: spaces, comma, +, & (keep it simple)
        payload = payload.replace(",", " ").replace("+", " ").replace("&", " ")
        parts = [p for p in payload.split() if p]

        if len(parts) < 2:
            # let handler show its usage (pass empty -> it will return usage)
            return handle_liebestest("", "")

        user_a = _strip_trailing_punct(parts[0]).lstrip("@")
        user_b = _strip_trailing_punct(parts[1]).lstrip("@")
        return handle_liebestest(user_a, user_b)

    # LLM explicit
    m = P_LLM.match(t)
    if m:
        query = m.group(1).strip()
        if "meinung" in t:
            return handle_opinion(query)
        return handle_explain(query)

    # fallback "bot sag <x>" -> explain
    m = P_SAG_ANY.match(t)
    if m:
        query = (m.group(1) or "").strip()
        if query:
            return handle_explain(query)

    return None
