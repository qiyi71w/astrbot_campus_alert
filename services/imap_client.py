from __future__ import annotations

import asyncio
import re
from collections.abc import Iterable


try:
    import aioimaplib
except ImportError:  # pragma: no cover - resolved in runtime environment
    aioimaplib = None


class ImapClientError(RuntimeError):
    pass


def _ensure_text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    return str(value)


def _flatten_lines(lines: Iterable) -> list:
    result = []
    for item in lines:
        if isinstance(item, (list, tuple)):
            result.extend(_flatten_lines(item))
        else:
            result.append(item)
    return result


def _extract_uid_marker_from_text(text: str) -> str | None:
    upper = text.upper()
    if "UID" not in upper:
        return None
    # Metadata lines usually contain FETCH or start with "*".
    if "FETCH" not in upper and not upper.lstrip().startswith("*"):
        return None
    match = re.search(r"\bUID\s+(\d+)\b", text, re.IGNORECASE)
    if not match:
        return None
    return match.group(1)


def _pick_best_email_chunk(chunks: list[bytes]) -> bytes | None:
    if not chunks:
        return None
    for item in chunks:
        if b"\r\n\r\n" in item and b":" in item:
            return item
    return max(chunks, key=len)


class AsyncImapClient:
    """Small compatibility wrapper around aioimaplib."""

    def __init__(self, host: str, port: int, use_ssl: bool = True, timeout: int = 30):
        self.host = host
        self.port = int(port)
        self.use_ssl = bool(use_ssl)
        self.timeout = max(5, int(timeout))
        self._client = None
        self._mailbox = "INBOX"
        self._connected = False

    async def connect(self, username: str, password: str, mailbox: str = "INBOX") -> None:
        if aioimaplib is None:
            raise ImapClientError(
                "aioimaplib is not installed. Please install plugin requirements.",
            )

        if self.use_ssl:
            self._client = aioimaplib.IMAP4_SSL(
                host=self.host,
                port=self.port,
                timeout=self.timeout,
            )
        else:
            self._client = aioimaplib.IMAP4(
                host=self.host,
                port=self.port,
                timeout=self.timeout,
            )

        await self._client.wait_hello_from_server()
        status, _ = self._normalize_response(await self._client.login(username, password))
        if not self._is_ok(status):
            raise ImapClientError(f"IMAP login failed: {status}")

        self._mailbox = mailbox or "INBOX"
        status, _ = self._normalize_response(await self._client.select(self._mailbox))
        if not self._is_ok(status):
            raise ImapClientError(f"IMAP select mailbox failed: {status}")

        self._connected = True

    async def close(self) -> None:
        if not self._client:
            return
        try:
            if hasattr(self._client, "logout"):
                await self._client.logout()
        except Exception:
            pass
        finally:
            self._connected = False
            self._client = None

    async def fetch_latest_emails(self, batch_size: int) -> list[tuple[str, bytes]]:
        if not self._connected or not self._client:
            raise ImapClientError("IMAP client is not connected")

        uids = await self._search_uids("ALL")
        if not uids:
            return []

        selected_uids = uids[-max(1, int(batch_size)) :]
        uid_set = self._build_uid_set(selected_uids)
        bulk_emails = await self._fetch_emails_by_uid_set(uid_set, selected_uids)
        bulk_map = {uid: raw for uid, raw in bulk_emails}

        if len(bulk_map) == len(selected_uids):
            return [(uid, bulk_map[uid]) for uid in selected_uids]

        # Fallback: fill missing UIDs one by one.
        emails: list[tuple[str, bytes]] = []
        for uid in selected_uids:
            if uid in bulk_map:
                emails.append((uid, bulk_map[uid]))
                continue
            raw = await self._fetch_email_by_uid(uid)
            if raw:
                emails.append((uid, raw))
        return emails

    async def _search_uids(self, criteria: str) -> list[str]:
        methods = []
        if hasattr(self._client, "uid_search"):
            methods.append(
                (
                    self._client.uid_search,
                    [(criteria,), (None, criteria), ("UTF-8", criteria)],
                ),
            )
        if hasattr(self._client, "uid"):
            methods.append(
                (
                    self._client.uid,
                    [
                        ("SEARCH", criteria),
                        ("SEARCH", None, criteria),
                        ("search", criteria),
                        ("search", None, criteria),
                    ],
                ),
            )

        for method, call_args in methods:
            for args in call_args:
                try:
                    status, lines = self._normalize_response(await method(*args))
                except TypeError:
                    continue
                if not self._is_ok(status):
                    continue
                uids = self._parse_uids(lines)
                if uids or lines == []:
                    return uids

        raise ImapClientError("Unable to search IMAP UIDs with current aioimaplib API")

    async def _fetch_emails_by_uid_set(
        self,
        uid_set: str,
        selected_uids: list[str],
    ) -> list[tuple[str, bytes]]:
        methods = []
        if hasattr(self._client, "uid_fetch"):
            methods.append(
                (
                    self._client.uid_fetch,
                    [
                        (uid_set, "(RFC822)"),
                        (uid_set, "RFC822"),
                        (uid_set, "BODY.PEEK[]"),
                    ],
                ),
            )
        if hasattr(self._client, "uid"):
            methods.append(
                (
                    self._client.uid,
                    [
                        ("FETCH", uid_set, "(RFC822)"),
                        ("FETCH", uid_set, "RFC822"),
                        ("FETCH", uid_set, "BODY.PEEK[]"),
                    ],
                ),
            )

        for method, call_args in methods:
            for args in call_args:
                try:
                    status, lines = self._normalize_response(await method(*args))
                except TypeError:
                    continue
                if not self._is_ok(status):
                    continue
                raw_map = self._extract_raw_emails(lines)
                if not raw_map:
                    continue
                return [
                    (uid, raw_map[uid]) for uid in selected_uids if uid in raw_map
                ]
        return []

    async def _fetch_email_by_uid(self, uid: str) -> bytes:
        methods = []
        if hasattr(self._client, "uid_fetch"):
            methods.append(
                (
                    self._client.uid_fetch,
                    [
                        (uid, "(RFC822)"),
                        (uid, "RFC822"),
                        (uid, "BODY.PEEK[]"),
                        (uid,),
                    ],
                ),
            )
        if hasattr(self._client, "uid"):
            methods.append(
                (
                    self._client.uid,
                    [
                        ("FETCH", uid, "(RFC822)"),
                        ("FETCH", uid, "RFC822"),
                        ("FETCH", uid, "BODY.PEEK[]"),
                        ("fetch", uid, "(RFC822)"),
                    ],
                ),
            )

        for method, call_args in methods:
            for args in call_args:
                try:
                    status, lines = self._normalize_response(await method(*args))
                except TypeError:
                    continue
                if not self._is_ok(status):
                    continue
                raw = self._extract_raw_email(lines)
                if raw:
                    return raw

        raise ImapClientError(f"Unable to fetch email content by UID {uid}")

    @staticmethod
    def _build_uid_set(uids: list[str]) -> str:
        if not uids:
            return ""

        numbers = []
        try:
            numbers = sorted({int(uid) for uid in uids})
        except ValueError:
            return ",".join(uids)

        ranges = []
        start = numbers[0]
        end = numbers[0]
        for value in numbers[1:]:
            if value == end + 1:
                end = value
                continue
            if start == end:
                ranges.append(str(start))
            else:
                ranges.append(f"{start}:{end}")
            start = value
            end = value

        if start == end:
            ranges.append(str(start))
        else:
            ranges.append(f"{start}:{end}")
        return ",".join(ranges)

    @staticmethod
    def _normalize_response(response) -> tuple[str, list]:
        if response is None:
            return "", []

        if isinstance(response, tuple) and len(response) >= 2:
            status = _ensure_text(response[0]).upper()
            lines = response[1]
            if not isinstance(lines, list):
                lines = [lines]
            return status, lines

        status = (
            _ensure_text(getattr(response, "result", ""))
            or _ensure_text(getattr(response, "status", ""))
        ).upper()
        lines = getattr(response, "lines", None)
        if lines is None:
            lines = getattr(response, "data", [])
        if lines is None:
            lines = []
        if not isinstance(lines, list):
            lines = [lines]
        return status, lines

    @staticmethod
    def _is_ok(status: str) -> bool:
        if not status:
            return True
        return status.startswith("OK")

    @staticmethod
    def _parse_uids(lines: list) -> list[str]:
        search_numbers: list[str] = []
        fallback_numbers: list[str] = []

        for line in _flatten_lines(lines):
            text = ""
            if isinstance(line, (bytes, bytearray)):
                text = line.decode("utf-8", errors="ignore")
            elif isinstance(line, str):
                text = line
            else:
                continue

            upper = text.upper()
            if "SEARCH" in upper:
                search_index = upper.find("SEARCH")
                tail = text[search_index + len("SEARCH") :]
                search_numbers.extend(re.findall(r"\b\d+\b", tail))
                continue

            # Fallback only for plain-number lines to avoid pulling unrelated digits.
            if re.fullmatch(r"\s*\d+(?:\s+\d+)*\s*", text):
                fallback_numbers.extend(re.findall(r"\b\d+\b", text))

        numbers = search_numbers if search_numbers else fallback_numbers
        seen = set()
        ordered: list[str] = []
        for item in numbers:
            if item in seen:
                continue
            seen.add(item)
            ordered.append(item)
        return ordered

    @staticmethod
    def _extract_raw_emails(lines: list) -> dict[str, bytes]:
        records: dict[str, bytes] = {}
        current_uid: str | None = None
        current_chunks: list[bytes] = []

        def flush_current() -> None:
            nonlocal current_uid, current_chunks
            if not current_uid:
                current_chunks = []
                return
            best = _pick_best_email_chunk(current_chunks)
            if best:
                records[current_uid] = best
            current_uid = None
            current_chunks = []

        for line in _flatten_lines(lines):
            text = ""
            raw_bytes: bytes | None = None

            if isinstance(line, (bytes, bytearray)):
                raw_bytes = bytes(line)
                text = raw_bytes.decode("utf-8", errors="ignore")
            elif isinstance(line, str):
                text = line

            uid_marker = _extract_uid_marker_from_text(text)
            if uid_marker:
                flush_current()
                current_uid = uid_marker
                continue

            if current_uid is None:
                continue
            if raw_bytes is not None:
                current_chunks.append(raw_bytes)
            elif isinstance(line, str) and ("\r\n\r\n" in line and ":" in line):
                current_chunks.append(line.encode("utf-8", errors="ignore"))

        flush_current()
        return records

    @staticmethod
    def _extract_raw_email(lines: list) -> bytes | None:
        candidates: list[bytes] = []
        for line in _flatten_lines(lines):
            if isinstance(line, (bytes, bytearray)):
                candidates.append(bytes(line))

        if not candidates:
            return None

        # Prefer payload with headers/body separators.
        for item in candidates:
            if b"\r\n\r\n" in item and b":" in item:
                return item

        # Fall back to the longest binary chunk.
        return max(candidates, key=len)

    async def wait_before_retry(self, seconds: float) -> None:
        await asyncio.sleep(max(0.0, float(seconds)))

