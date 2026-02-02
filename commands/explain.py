from utils.llm_groq import groq_chat

def handle_explain(query: str) -> str:
    q = (query or "").strip()
    if not q:
        return "Bitte: bot erkläre <thema>"

    system = (
        "Du erklärst Dinge kurz, korrekt und verständlich auf Deutsch. "
        "Maximal 8 Sätze. Wenn nötig, nutze eine kurze Liste."
    )
    prompt = f"Erkläre: {q}"

    try:
        out = groq_chat(prompt=prompt, system=system, temperature=0.2, max_tokens=280)
        return out if out else "Keine Antwort erhalten."
    except Exception:
        return "LLM gerade nicht verfügbar (API/Quota/Key)."
