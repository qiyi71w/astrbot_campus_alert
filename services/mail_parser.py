from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from email import policy
from email.header import decode_header, make_header
from email.parser import BytesParser
from email.utils import parseaddr, parsedate_to_datetime
from html import unescape


_RE_SCRIPT = re.compile(r"<(script|style)\b[^>]*>.*?</\1>", re.IGNORECASE | re.DOTALL)
_RE_TAG = re.compile(r"<[^>]+>")
_RE_SPACE = re.compile(r"\s+")


@dataclass
class ParsedMail:
    uid: str
    message_id: str
    sender: str
    sender_addr: str
    subject: str
    raw_date: str
    mail_datetime_utc: datetime | None
    body_text: str


def _decode_header_value(value: str | None) -> str:
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value))).strip()
    except Exception:
        return value.strip()


def _to_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _decode_bytes(payload: bytes, charset: str | None) -> str:
    charsets = [charset, "utf-8", "gb18030", "latin-1"]
    for item in charsets:
        if not item:
            continue
        try:
            return payload.decode(item, errors="replace")
        except Exception:
            continue
    return payload.decode("utf-8", errors="replace")


def _clean_html(html: str) -> str:
    text = _RE_SCRIPT.sub(" ", html)
    text = _RE_TAG.sub(" ", text)
    text = unescape(text)
    text = _RE_SPACE.sub(" ", text).strip()
    return text


def _extract_mail_body(message) -> str:
    plain_candidates: list[str] = []
    html_candidates: list[str] = []

    if message.is_multipart():
        for part in message.walk():
            content_type = (part.get_content_type() or "").lower()
            disposition = (part.get("Content-Disposition") or "").lower()
            if "attachment" in disposition:
                continue
            payload = part.get_payload(decode=True)
            if not payload:
                continue
            text = _decode_bytes(payload, part.get_content_charset())
            if content_type == "text/plain":
                plain_candidates.append(text.strip())
            elif content_type == "text/html":
                html_candidates.append(_clean_html(text))
    else:
        payload = message.get_payload(decode=True)
        if payload:
            text = _decode_bytes(payload, message.get_content_charset())
            content_type = (message.get_content_type() or "").lower()
            if content_type == "text/html":
                html_candidates.append(_clean_html(text))
            else:
                plain_candidates.append(text.strip())

    if plain_candidates:
        merged = "\n".join(item for item in plain_candidates if item)
        return _RE_SPACE.sub(" ", merged).strip()
    if html_candidates:
        merged = "\n".join(item for item in html_candidates if item)
        return _RE_SPACE.sub(" ", merged).strip()
    return ""


def parse_mail(raw_bytes: bytes, uid: str, max_body_chars: int) -> ParsedMail:
    message = BytesParser(policy=policy.default).parsebytes(raw_bytes)

    subject = _decode_header_value(message.get("Subject"))
    sender = _decode_header_value(message.get("From"))
    sender_addr = parseaddr(sender)[1].strip().lower()
    message_id = (message.get("Message-ID") or "").strip()
    raw_date = (message.get("Date") or "").strip()
    body_text = _extract_mail_body(message)
    max_len = max(100, int(max_body_chars))
    if len(body_text) > max_len:
        body_text = body_text[:max_len]

    return ParsedMail(
        uid=str(uid),
        message_id=message_id,
        sender=sender,
        sender_addr=sender_addr,
        subject=subject,
        raw_date=raw_date,
        mail_datetime_utc=_to_utc(raw_date),
        body_text=body_text,
    )


def build_fallback_message_key(parsed_mail: ParsedMail) -> str:
    source = (
        f"{parsed_mail.sender_addr}|{parsed_mail.subject}|"
        f"{parsed_mail.raw_date}|{parsed_mail.uid}"
    )
    digest = hashlib.sha256(source.encode("utf-8", errors="ignore")).hexdigest()
    return f"fallback:{digest}"


def in_alert_window(
    mail_time_utc: datetime | None,
    now_utc: datetime,
    alert_window_hours: int,
) -> bool:
    if mail_time_utc is None:
        return False
    delta_seconds = (now_utc - mail_time_utc).total_seconds()
    return 0 <= delta_seconds <= max(1, int(alert_window_hours)) * 3600

