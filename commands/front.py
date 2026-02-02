import random

def handle_front(target: str = "") -> str:
    target = (target or "").strip()

    if target:
        options = [
            f"{target} deine Eltern sind Geschwister!.",
            f"{target} du Hundesohn!",
            f"{target}? Fickfehler",
            f"{target} du dummer Zigeuner!",
            f"{target} du Neger",
            f"{target} du Hure",
            f"{target} du Alman",
            f"{target} du bist ab jetzt Vogelfrei!",
        ]
        return random.choice(options)

    options = [
        "Deine Eltern sind Geschwister!",
        "Du Hundesohn!",
        "Fickfehler",
        "Toony ist ein Hurensohn",
        "Fotze!",
        "Arschloch!",
        "Pimmel!",
        "Hundesohn!",
        "Scheißkerl!",
        "Blöder Penner!",
        "Hure!",
        "Dreckschwein!",
        "Dummer Zigeuner!",
        "Krüppel!",
        "Harzer!",
        "Hässlicher Freak!",
        "Neger!",
        "Pollacke!",
        "Dio porco!",
        "Ich scheiß auf deine Synagoge!",
        "Der Koran lügt!",
        "Ganz dünnes Eis!",
        "Heute nicht, mein Freund.",
        "Mach langsam, Chef.",
        "Stark. Wirklich stark.",
    ]
    return random.choice(options)
