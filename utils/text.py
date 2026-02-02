import re

_TAG_RE = re.compile(r"<[^>]+>")

def strip_html(text: str) -> str:
    if not text:
        return ""
    # Disqus message often includes <p>...</p>
    t = _TAG_RE.sub(" ", text)
    t = " ".join(t.split())
    return t.strip()
