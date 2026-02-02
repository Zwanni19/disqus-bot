import os
import requests

GROQ_BASE_URL = "https://api.groq.com/openai/v1"

def groq_chat(prompt: str, system: str = "", temperature: float = 0.2, max_tokens: int = 220) -> str:
    api_key = (os.environ.get("GROQ_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("Missing GROQ_API_KEY env var.")

    model = (os.environ.get("GROQ_MODEL") or "llama-3.3-70b-versatile").strip()

    url = f"{GROQ_BASE_URL}/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    payload = {
        "model": model,
        "messages": messages,
        "temperature": float(temperature),
        "max_tokens": int(max_tokens),
    }

    r = requests.post(url, headers=headers, json=payload, timeout=20)
    r.raise_for_status()
    j = r.json()

    return (((j.get("choices") or [])[0].get("message") or {}).get("content") or "").strip()
