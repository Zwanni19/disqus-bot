import requests


def handle_joke() -> str:
    """
    Random German joke via JokeAPI.
    Uses safe-mode + blacklistFlags to avoid NSFW/explicit content.
    """
    base = "https://v2.jokeapi.dev/joke/Any"
    params = {
        "lang": "de",
        "blacklistFlags": "nsfw,religious,racist,sexist,explicit",
        "format": "json",
    }

    try:
        r = requests.get(base, params=params, timeout=10)
        url = r.url
        if "safe-mode" not in url:
            url = url + "&safe-mode"
        r = requests.get(url, timeout=10)

        data = r.json()
        if data.get("error"):
            return "Kein Witz gefunden."

        t = data.get("type")
        if t == "single":
            return (data.get("joke") or "Kein Witz gefunden.").strip()

        if t == "twopart":
            setup = (data.get("setup") or "").strip()
            delivery = (data.get("delivery") or "").strip()
            if setup and delivery:
                return f"{setup}\n{delivery}"
            return setup or delivery or "Kein Witz gefunden."

        return "Kein Witz gefunden."
    except Exception:
        return "Witz-API gerade nicht erreichbar."
