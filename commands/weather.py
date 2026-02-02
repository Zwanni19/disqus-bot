import requests

def handle_weather(city: str) -> str:
    city = (city or "").strip()
    if not city:
        return "Bitte: bot sag wetter in <stadt>"

    # 1) geocode
    geo = requests.get(
        "https://geocoding-api.open-meteo.com/v1/search",
        params={"name": city, "count": 1, "language": "de", "format": "json"},
        timeout=20,
    )
    geo.raise_for_status()
    gj = geo.json()
    results = gj.get("results") or []
    if not results:
        return f"Ort nicht gefunden: {city}"

    r0 = results[0]
    lat = r0["latitude"]
    lon = r0["longitude"]
    name = r0.get("name", city)
    country = r0.get("country", "")

    # 2) current weather
    w = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={"latitude": lat, "longitude": lon, "current_weather": True},
        timeout=20,
    )
    w.raise_for_status()
    wj = w.json()
    cw = (wj.get("current_weather") or {})
    temp = cw.get("temperature")
    wind = cw.get("windspeed")

    where = f"{name}" + (f", {country}" if country else "")
    if temp is None:
        return f"{where}: Wetter aktuell nicht verfügbar, zieh zur Sicherheit eine Hose an!"

    if wind is not None:
        return f"{where}: {temp}°C, Wind {wind} km/h"
    return f"{where}: {temp}°C"
