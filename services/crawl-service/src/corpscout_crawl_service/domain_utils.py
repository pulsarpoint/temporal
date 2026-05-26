from __future__ import annotations

import re
import urllib.parse


_VALID_HOST_RE = re.compile(
    r"^(localhost|(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z]{2,63})$"
)


def normalize_domain(value: str | None) -> str:
    if not value:
        return ""
    text = value.strip()
    if not text:
        return ""
    if "://" in text:
        parsed = urllib.parse.urlparse(text)
        text = parsed.netloc or parsed.path
    else:
        text = text.split("/", 1)[0]
    text = text.split("@")[-1].split(":", 1)[0].strip(".").lower()
    if text.startswith("www."):
        text = text[4:]
    return text


def domain_from_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    return normalize_domain(parsed.netloc)


def normalize_url(value: str) -> str:
    text = value.strip()
    if not text:
        return ""
    if any(char.isspace() for char in text):
        return ""
    if re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:", text) and not re.match(r"^https?://", text, flags=re.IGNORECASE):
        return ""
    if not re.match(r"^https?://", text, flags=re.IGNORECASE):
        text = "https://" + text
    parsed = urllib.parse.urlparse(text)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return ""
    try:
        parsed.port
    except ValueError:
        return ""
    host = parsed.hostname
    if not host or not _VALID_HOST_RE.match(host):
        return ""
    return text


def unwrap_search_result_url(value: str) -> str:
    normalized = normalize_url(value)
    if not normalized:
        return ""
    parsed = urllib.parse.urlparse(normalized)
    params = urllib.parse.parse_qs(parsed.query)
    for key in ("uddg", "url", "u", "target", "to"):
        for raw_target in params.get(key) or []:
            target = normalize_url(urllib.parse.unquote(raw_target))
            if target:
                return target
    return normalized
