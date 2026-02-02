import random

def handle_liebestest(user_a: str, user_b: str) -> str:
    a = (user_a or "").strip()
    b = (user_b or "").strip()

    if not a or not b:
        return "Nutzung: bot sag liebestest <UserA> <UserB>"

    if a.lower() == b.lower():
        pct = 100
    else:
        pct = random.randint(0, 100)

    return f"❤️ Liebestest {a} + {b}: {pct}%"
