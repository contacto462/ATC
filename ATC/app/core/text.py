from __future__ import annotations

from email.header import decode_header


def decode_mime_words(value: str | None) -> str:
    if not value:
        return ""

    try:
        parts = decode_header(value)
    except Exception:
        return value.strip() if isinstance(value, str) else ""

    out: list[str] = []
    for part, enc in parts:
        if isinstance(part, bytes):
            out.append(part.decode(enc or "utf-8", errors="ignore"))
        else:
            out.append(str(part))
    return "".join(out).strip()
