import random

def _build_values():
    values = []
    values += [round(i / 10, 1) for i in range(1, 10)]  # 0.1..0.9
    values += [x / 2 for x in range(2, 71)]            # 1.0..35.0 step 0.5
    return values

_VALUES = _build_values()

def handle_size() -> str:
    value = random.choice(_VALUES)
    s = f"{value:.1f}".replace(".", ",")
    return f"{s} cm"
