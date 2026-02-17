from __future__ import annotations

import asyncio
import imaplib
import inspect
import re
from collections.abc import Iterable
from urllib.parse import unquote, urlparse

from astrbot.api import logger


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
    compact = upper.lstrip()
    if (
        "FETCH" not in upper
        and not compact.startswith("*")
        and not re.match(r"^\d+\s+\(", compact)
    ):
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


def _normalize_mailbox_name(mailbox: str | None) -> str:
    raw = (mailbox or "INBOX").strip()
    if not raw:
        return "INBOX"

    lower = raw.lower()
    if lower.startswith("imap://") or lower.startswith("imaps://"):
        parsed = urlparse(raw)
        path = unquote(parsed.path or "").strip()
        if not path:
            return "INBOX"
        path = path.strip("/")
        if not path:
            return "INBOX"
        return path.split("/")[-1] or "INBOX"

    return raw


class AsyncImapClient:
    """Small compatibility wrapper around aioimaplib."""

    def __init__(self, host: str, port: int, use_ssl: bool = True, timeout: int = 30):
        self.host = host
        self.port = int(port)
        self.use_ssl = bool(use_ssl)
        self.timeout = max(5, int(timeout))
        self._client = None
        self._sync_client: imaplib.IMAP4 | None = None
        self._mailbox = "INBOX"
        self._connected = False
        self._id_available_methods: list[str] = []
        self._active_fetch_mode = ""

    async def connect(self, username: str, password: str, mailbox: str = "INBOX") -> None:
        self._mailbox = _normalize_mailbox_name(mailbox)

        if self._should_prefer_std_imap():
            logger.info(
                "[CampusAlert] 检测到网易系 IMAP 主机，优先尝试 stdlib 连接。host=%s",
                self.host,
            )
            fallback_ok, fallback_note = await self._try_connect_with_std_imap(
                username=username,
                password=password,
                mailbox=self._mailbox,
            )
            if fallback_ok:
                self._connected = True
                self._active_fetch_mode = "stdlib-fallback"
                logger.info("[CampusAlert] IMAP stdlib 优先连接成功。%s", fallback_note)
                return
            logger.warning(
                "[CampusAlert] IMAP stdlib 优先连接失败，回退 aioimaplib。%s",
                fallback_note,
            )

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

        # Some providers (notably NetEase family) may require ID handshake.
        self._id_available_methods = self._collect_id_method_names()
        id_sent_before_login = await self._send_client_id_if_supported(contact=username)

        status, _ = self._normalize_response(await self._client.login(username, password))
        if not self._is_ok(status):
            raise ImapClientError(f"IMAP login failed: {status}")

        # Retry ID in authenticated state for clients/servers that expect it here.
        id_sent_after_login = await self._send_client_id_if_supported(contact=username)
        logger.info(
            "[CampusAlert] IMAP ID 握手结果 pre=%s post=%s methods=%s",
            id_sent_before_login,
            id_sent_after_login,
            ",".join(self._id_available_methods) or "-",
        )

        selected_mailbox, select_status, select_lines = await self._select_mailbox_with_fallback(
            self._mailbox,
        )
        if not selected_mailbox:
            detail = self._format_lines_excerpt(select_lines)
            unsafe_login = "UNSAFE LOGIN" in detail.upper()
            fallback_note = "-"
            if unsafe_login:
                logger.warning(
                    "[CampusAlert] IMAP SELECT 命中 Unsafe Login，尝试 stdlib 回退。mailbox=%s detail=%s",
                    self._mailbox,
                    detail,
                )
                fallback_ok, fallback_note = await self._try_connect_with_std_imap(
                    username=username,
                    password=password,
                    mailbox=self._mailbox,
                )
                if fallback_ok:
                    self._connected = True
                    self._active_fetch_mode = "stdlib-fallback"
                    logger.info("[CampusAlert] IMAP stdlib 回退连接成功。%s", fallback_note)
                    return
                logger.warning("[CampusAlert] IMAP stdlib 回退失败。%s", fallback_note)
            raise ImapClientError(
                "IMAP select mailbox failed: "
                f"{select_status}; mailbox={self._mailbox}; "
                f"id_pre={id_sent_before_login}; id_post={id_sent_after_login}; "
                f"id_methods={','.join(self._id_available_methods) or '-'}; "
                f"detail={detail}; std_imap_fallback={fallback_note}",
            )
        self._mailbox = selected_mailbox

        self._connected = True
        self._active_fetch_mode = "aioimaplib"
        logger.info("[CampusAlert] IMAP 原生连接成功。mailbox=%s", self._mailbox)

    async def close(self) -> None:
        if self._sync_client:
            sync_client = self._sync_client
            self._sync_client = None
            try:
                await asyncio.to_thread(sync_client.logout)
            except Exception:
                pass

        if self._client:
            try:
                if hasattr(self._client, "logout"):
                    await self._client.logout()
            except Exception:
                pass
            finally:
                self._client = None

        self._connected = False
        self._active_fetch_mode = ""

    async def _close_async_client_only(self) -> None:
        if not self._client:
            return
        client = self._client
        self._client = None
        try:
            if hasattr(client, "logout"):
                await client.logout()
        except Exception as exc:
            logger.debug("[CampusAlert] 清理 aioimaplib 连接失败（忽略）：%s", exc)

    async def fetch_latest_emails(self, batch_size: int) -> list[tuple[str, bytes]]:
        if not self._connected:
            raise ImapClientError("IMAP client is not connected")

        if self._sync_client is not None:
            if self._active_fetch_mode != "stdlib-fallback":
                self._active_fetch_mode = "stdlib-fallback"
                logger.info("[CampusAlert] 当前邮件拉取路径：stdlib 回退链路")
            return await self._fetch_latest_emails_sync(batch_size)

        if not self._client:
            raise ImapClientError("IMAP client is not connected")
        if self._active_fetch_mode != "aioimaplib":
            self._active_fetch_mode = "aioimaplib"
            logger.info("[CampusAlert] 当前邮件拉取路径：aioimaplib 原生链路")

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

    async def _fetch_latest_emails_sync(self, batch_size: int) -> list[tuple[str, bytes]]:
        uids = await self._search_uids_sync("ALL")
        if not uids:
            return []

        selected_uids = uids[-max(1, int(batch_size)) :]
        uid_set = self._build_uid_set(selected_uids)
        bulk_map = await self._fetch_raw_emails_by_uid_set_sync(uid_set)

        emails: list[tuple[str, bytes]] = []
        for uid in selected_uids:
            raw = bulk_map.get(uid)
            if raw is None:
                raw = await self._fetch_email_by_uid_sync(uid)
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

    async def _search_uids_sync(self, criteria: str) -> list[str]:
        for args in ((None, criteria), (criteria,)):
            status, lines = await self._sync_uid_command("SEARCH", *args)
            if status == "OK":
                return self._parse_uids(lines)
        raise ImapClientError("IMAP UID SEARCH failed in sync fallback mode")

    async def _fetch_raw_emails_by_uid_set_sync(self, uid_set: str) -> dict[str, bytes]:
        if not uid_set:
            return {}
        status, lines = await self._sync_uid_command("FETCH", uid_set, "(RFC822)")
        if status != "OK":
            return {}
        return self._extract_raw_emails(lines)

    async def _fetch_email_by_uid_sync(self, uid: str) -> bytes:
        status, lines = await self._sync_uid_command("FETCH", uid, "(RFC822)")
        if status != "OK":
            return b""
        return self._extract_raw_email(lines) or b""

    async def _sync_uid_command(self, command: str, *args) -> tuple[str, list]:
        if not self._sync_client:
            raise ImapClientError("sync IMAP client is not connected")

        def _run():
            return self._sync_client.uid(command, *args)

        typ, data = await asyncio.to_thread(_run)
        status = _ensure_text(typ).upper()
        if data is None:
            lines = []
        elif isinstance(data, list):
            lines = data
        else:
            lines = [data]
        return status, lines

    async def _try_connect_with_std_imap(
        self,
        *,
        username: str,
        password: str,
        mailbox: str,
    ) -> tuple[bool, str]:
        if self.use_ssl:
            client_cls = imaplib.IMAP4_SSL
        else:
            client_cls = imaplib.IMAP4

        client: imaplib.IMAP4 | None = None
        try:
            client = await asyncio.to_thread(client_cls, self.host, self.port)
            typ, _ = await asyncio.to_thread(client.login, username, password)
            if _ensure_text(typ).upper() != "OK":
                await asyncio.to_thread(client.logout)
                return False, f"sync_login={typ}"

            id_ok = await asyncio.to_thread(
                self._send_id_with_std_imap,
                client,
                username,
            )

            selected = ""
            last_typ = ""
            last_data = []
            for candidate in self._build_mailbox_candidates(mailbox):
                typ, data = await asyncio.to_thread(client.select, candidate)
                last_typ = _ensure_text(typ).upper()
                last_data = data if isinstance(data, list) else [data]
                if last_typ == "OK":
                    selected = candidate
                    break

            if not selected:
                await asyncio.to_thread(client.logout)
                detail = self._format_lines_excerpt(last_data)
                return False, f"sync_select={last_typ}; detail={detail}; id={id_ok}"

            # If this call is used as fallback after aioimaplib already connected,
            # release that connection before switching to stdlib path.
            await self._close_async_client_only()
            # hand over ownership to async wrapper
            self._sync_client = client
            self._mailbox = selected
            self._client = None
            return True, f"id={id_ok}; mailbox={selected}"
        except Exception as exc:
            if client is not None:
                try:
                    await asyncio.to_thread(client.logout)
                except Exception:
                    pass
            return False, f"sync_exception={type(exc).__name__}: {exc}"

    @staticmethod
    def _send_id_with_std_imap(client: imaplib.IMAP4, contact: str) -> bool:
        payload = (
            f'("name" "Thunderbird" "version" "115.0" '
            f'"vendor" "Mozilla" "contact" "{contact or "unknown"}")'
        )
        try:
            typ, _ = client.xatom("ID", payload)
            if _ensure_text(typ).upper() == "OK":
                return True
        except Exception:
            pass

        try:
            tag = client._simple_command("ID", payload)  # type: ignore[attr-defined]
            typ, _ = client._command_complete("ID", tag)  # type: ignore[attr-defined]
            return _ensure_text(typ).upper() == "OK"
        except Exception:
            return False

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

    async def _send_client_id_if_supported(self, *, contact: str = "") -> bool:
        if not self._client:
            return False

        contact_value = (contact or "unknown").strip() or "unknown"
        id_pairs = [
            ("name", "Thunderbird"),
            ("version", "115.0"),
            ("vendor", "Mozilla"),
            ("contact", contact_value),
        ]
        payload = "(" + " ".join(f'"{k}" "{v}"' for k, v in id_pairs) + ")"
        pair_dict = dict(id_pairs)

        async def _attempt(method, *args, **kwargs) -> bool:
            try:
                response = method(*args, **kwargs)
                if inspect.isawaitable(response):
                    response = await response
            except Exception:
                return False
            status, _ = self._normalize_response(response)
            return self._is_ok(status)

        def _get_callable(name: str):
            method = getattr(self._client, name, None)
            if callable(method):
                return method
            return None

        # Keep only proven paths:
        # 1) Native id()/ID() API if present.
        for name in ("id", "ID"):
            method = _get_callable(name)
            if not method:
                continue
            if await _attempt(method, pair_dict):
                return True
            if await _attempt(method, payload):
                return True

        # 2) Command-style fallback.
        for name in ("simple_command", "_simple_command", "xatom"):
            method = _get_callable(name)
            if not method:
                continue
            if await _attempt(method, "ID", payload):
                return True
        return False

    def _collect_id_method_names(self) -> list[str]:
        if not self._client:
            return []
        candidates = [
            "id",
            "ID",
            "simple_command",
            "_simple_command",
            "xatom",
        ]
        available = []
        for name in candidates:
            method = getattr(self._client, name, None)
            if callable(method):
                available.append(name)
        return available

    async def _select_mailbox_with_fallback(self, mailbox: str) -> tuple[str, str, list]:
        candidates = self._build_mailbox_candidates(mailbox)
        last_status = ""
        last_lines: list = []
        for candidate in candidates:
            status, lines = self._normalize_response(await self._client.select(candidate))
            last_status = status
            last_lines = lines
            if self._is_ok(status):
                return candidate, status, lines
        return "", last_status, last_lines

    @staticmethod
    def _build_mailbox_candidates(mailbox: str) -> list[str]:
        base = _normalize_mailbox_name(mailbox)
        candidates = [base]
        unquoted = base.strip('"').strip("'").strip()
        if unquoted and unquoted != base:
            candidates.append(unquoted)
        if unquoted and " " in unquoted:
            candidates.append(f'"{unquoted}"')
        if unquoted.upper() == "INBOX":
            candidates.extend(["INBOX", "Inbox", "inbox"])

        # Keep order and remove duplicates.
        deduped: list[str] = []
        seen = set()
        for item in candidates:
            if not item or item in seen:
                continue
            seen.add(item)
            deduped.append(item)
        return deduped

    def _should_prefer_std_imap(self) -> bool:
        host = (self.host or "").strip().lower()
        return host in {"imap.163.com", "imap.126.com", "imap.yeah.net"}

    @staticmethod
    def _format_lines_excerpt(lines: list, max_parts: int = 3) -> str:
        parts: list[str] = []
        for line in _flatten_lines(lines):
            text = _ensure_text(line).strip()
            if not text:
                continue
            parts.append(text)
            if len(parts) >= max_parts:
                break
        return " | ".join(parts) if parts else "-"
