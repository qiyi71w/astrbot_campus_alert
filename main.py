from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parseaddr
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Plain
from astrbot.api.star import Context, Star, StarTools

from .services.alert_classifier import AlertClassifier, AlertDecision
from .services.imap_client import AsyncImapClient, ImapClientError
from .services.mail_parser import (
    ParsedMail,
    build_fallback_message_key,
    in_alert_window,
    parse_mail,
)
from .services.storage import ProcessedEmailStore


DEFAULT_PROMPT_TEMPLATE = ""


CATEGORY_LABELS = {
    "security_alert": "紧急安全预警",
    "class_cancel": "停课通知",
    "public_health": "公共卫生提醒",
    "other": "其他",
}


def _to_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "on"}:
            return True
        if lowered in {"false", "0", "no", "off"}:
            return False
    return default


def _to_int(value: Any, default: int, min_value: int, max_value: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    if parsed < min_value:
        return min_value
    if parsed > max_value:
        return max_value
    return parsed


def _to_str(value: Any, default: str = "") -> str:
    if isinstance(value, str):
        return value.strip()
    if value is None:
        return default
    return str(value).strip()


def _to_str_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    result = []
    for item in value:
        text = _to_str(item)
        if text:
            result.append(text)
    return result


@dataclass
class PluginSettings:
    enabled: bool
    imap_host: str
    imap_port: int
    imap_use_ssl: bool
    imap_username: str
    imap_password: str
    imap_mailbox: str
    poll_interval_sec: int
    sender_allowlist: set[str]
    target_sessions: list[str]
    command_admin_only: bool
    alert_window_hours: int
    max_body_chars: int
    max_summary_chars: int
    fetch_batch_size: int
    bootstrap_mode: str
    llm_provider_id: str
    llm_prompt_template: str
    llm_retry_limit: int
    network_retry_base_sec: int
    network_retry_max_sec: int
    test_sender: str
    test_subject: str
    test_body: str
    debug_log: bool

    @classmethod
    def from_config(cls, config: dict | None) -> "PluginSettings":
        source = config or {}
        bootstrap_mode = _to_str(source.get("bootstrap_mode"), "silent_index").lower()
        if bootstrap_mode not in {"silent_index", "normal", "latest_one"}:
            bootstrap_mode = "silent_index"

        sender_allowlist = {
            item.lower() for item in _to_str_list(source.get("sender_allowlist"))
        }
        if not sender_allowlist:
            sender_allowlist = {"no-reply@alertable.ca"}

        return cls(
            enabled=_to_bool(source.get("enabled"), False),
            imap_host=_to_str(source.get("imap_host")),
            imap_port=_to_int(source.get("imap_port"), 993, 1, 65535),
            imap_use_ssl=_to_bool(source.get("imap_use_ssl"), True),
            imap_username=_to_str(source.get("imap_username")),
            imap_password=_to_str(source.get("imap_password")),
            imap_mailbox=_to_str(source.get("imap_mailbox"), "INBOX") or "INBOX",
            poll_interval_sec=_to_int(source.get("poll_interval_sec"), 60, 5, 3600),
            sender_allowlist=sender_allowlist,
            target_sessions=_to_str_list(source.get("target_sessions")),
            command_admin_only=_to_bool(source.get("command_admin_only"), True),
            alert_window_hours=_to_int(source.get("alert_window_hours"), 24, 1, 168),
            max_body_chars=_to_int(source.get("max_body_chars"), 2000, 100, 10000),
            max_summary_chars=_to_int(source.get("max_summary_chars"), 100, 10, 300),
            fetch_batch_size=_to_int(source.get("fetch_batch_size"), 20, 1, 100),
            bootstrap_mode=bootstrap_mode,
            llm_provider_id=_to_str(source.get("llm_provider_id")),
            llm_prompt_template=_to_str(
                source.get("llm_prompt_template"),
                DEFAULT_PROMPT_TEMPLATE,
            ),
            llm_retry_limit=_to_int(source.get("llm_retry_limit"), 3, 1, 6),
            network_retry_base_sec=_to_int(
                source.get("network_retry_base_sec"),
                5,
                1,
                600,
            ),
            network_retry_max_sec=_to_int(
                source.get("network_retry_max_sec"),
                300,
                5,
                3600,
            ),
            test_sender=_to_str(
                source.get("test_sender"),
                "no-reply@alertable.ca",
            ),
            test_subject=_to_str(
                source.get("test_subject"),
                "[TEST] CampusAlert 模拟告警邮件",
            ),
            test_body=_to_str(
                source.get("test_body"),
                "这是一封用于 CampusAlert 插件联调的测试邮件。"
                "请模拟校园安全告警流程并进行判定。",
            ),
            debug_log=_to_bool(source.get("debug_log"), False),
        )

    def has_required_imap(self) -> bool:
        return all([self.imap_host, self.imap_username, self.imap_password])


class CampusAlertPlugin(Star):
    """监控校园告警邮箱，使用 LLM 识别高优先级通知并推送到指定会话。"""

    def __init__(self, context: Context, config: dict | None = None):
        super().__init__(context)
        self.config = config or {}
        self.settings = PluginSettings.from_config(self.config)

        data_dir = StarTools.get_data_dir()
        self.store = ProcessedEmailStore(data_dir=data_dir)
        self.classifier = AlertClassifier(context=self.context)
        self.imap_client: AsyncImapClient | None = None
        self.imap_signature: tuple | None = None

        self._stop_event = asyncio.Event()
        self._wake_event = asyncio.Event()
        self._poll_lock = asyncio.Lock()
        self._monitor_task: asyncio.Task | None = None
        self._reload_requested = False
        self._bootstrap_pending = False

        self.runtime_state = "starting"
        self.last_poll_at: datetime | None = None
        self.last_error = ""
        self.backoff_seconds = 0
        self.stats = {
            "cycles": 0,
            "fetched": 0,
            "eligible": 0,
            "pushed": 0,
            "ignored": 0,
            "bootstrap_indexed": 0,
            "llm_failed": 0,
            "push_failed": 0,
        }

    @staticmethod
    def _state_text(state: str) -> str:
        mapping = {
            "starting": "启动中",
            "idle": "空闲",
            "disabled": "已停用",
            "config_error": "配置错误",
            "running": "运行中",
            "error": "异常",
        }
        return mapping.get(state, state)

    @staticmethod
    def _format_dt_utc(value: datetime | None) -> str:
        if value is None:
            return "暂无"
        return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    def _snapshot_stats(self) -> dict[str, int]:
        return {key: int(value) for key, value in self.stats.items()}

    def _refresh_settings(self) -> PluginSettings:
        self.settings = PluginSettings.from_config(self.config)
        return self.settings

    async def initialize(self):
        self.store.load()
        self._bootstrap_pending = self.store.first_bootstrap
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info(
            "[CampusAlert] 插件已初始化。首次启动=%s，去重记录=%s",
            self._bootstrap_pending,
            self.store.count(),
        )

    async def terminate(self):
        self._stop_event.set()
        self._wake_event.set()
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        await self._close_imap_client()
        self.store.save(force=True)
        logger.info("[CampusAlert] 插件已停止。")

    async def _monitor_loop(self):
        await asyncio.sleep(5)
        async with self._poll_lock:
            self.runtime_state = "idle"
        while not self._stop_event.is_set():
            self._refresh_settings()

            if self._reload_requested:
                async with self._poll_lock:
                    await self._close_imap_client()
                self._reload_requested = False

            if not self.settings.enabled:
                async with self._poll_lock:
                    self.runtime_state = "disabled"
                await self._wait_or_wake(self.settings.poll_interval_sec)
                continue

            if not self.settings.has_required_imap():
                async with self._poll_lock:
                    self.runtime_state = "config_error"
                    self.last_error = "IMAP configuration missing required fields."
                await self._wait_or_wake(self.settings.poll_interval_sec)
                continue

            try:
                async with self._poll_lock:
                    await self._ensure_imap_client()
                    fetched_count = await self._poll_once()
                    self.stats["cycles"] += 1
                    self.stats["fetched"] += fetched_count
                    self.last_poll_at = datetime.now(timezone.utc)
                    self.last_error = ""
                    self.runtime_state = "running"
                    self.backoff_seconds = self.settings.network_retry_base_sec
                await self._wait_or_wake(self.settings.poll_interval_sec)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                async with self._poll_lock:
                    self.runtime_state = "error"
                    self.last_error = f"{type(exc).__name__}: {exc}"
                    logger.error("[CampusAlert] Monitor loop error: %s", self.last_error)
                    await self._close_imap_client()
                    wait_seconds = (
                        self.backoff_seconds
                        if self.backoff_seconds > 0
                        else self.settings.network_retry_base_sec
                    )
                    self.backoff_seconds = min(
                        max(self.settings.network_retry_base_sec, wait_seconds * 2),
                        self.settings.network_retry_max_sec,
                    )
                await self._wait_or_wake(wait_seconds)

    async def _wait_or_wake(self, seconds: int):
        self._wake_event.clear()
        if seconds <= 0:
            return
        try:
            await asyncio.wait_for(self._wake_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def _ensure_imap_client(self):
        assert self.settings
        signature = (
            self.settings.imap_host,
            self.settings.imap_port,
            self.settings.imap_use_ssl,
            self.settings.imap_username,
            self.settings.imap_password,
            self.settings.imap_mailbox,
        )
        if self.imap_client and self.imap_signature == signature:
            return

        await self._close_imap_client()

        self.imap_client = AsyncImapClient(
            host=self.settings.imap_host,
            port=self.settings.imap_port,
            use_ssl=self.settings.imap_use_ssl,
            timeout=30,
        )
        await self.imap_client.connect(
            username=self.settings.imap_username,
            password=self.settings.imap_password,
            mailbox=self.settings.imap_mailbox,
        )
        self.imap_signature = signature
        logger.info(
            "[CampusAlert] IMAP 已连接。主机=%s，邮箱文件夹=%s",
            self.settings.imap_host,
            self.settings.imap_mailbox,
        )

    async def _close_imap_client(self):
        if self.imap_client:
            await self.imap_client.close()
        self.imap_client = None
        self.imap_signature = None

    async def _poll_once(self) -> int:
        if not self.imap_client:
            raise ImapClientError("IMAP client not ready")

        mails = await self.imap_client.fetch_latest_emails(self.settings.fetch_batch_size)
        if not mails:
            if self._bootstrap_pending:
                self._bootstrap_pending = False
            return 0

        now_utc = datetime.now(timezone.utc)
        eligible_items = []
        unique_fetched_count = 0

        for uid, raw_mail in mails:
            try:
                parsed = parse_mail(raw_mail, uid=uid, max_body_chars=self.settings.max_body_chars)
            except Exception as exc:
                self.stats["ignored"] += 1
                logger.warning("[CampusAlert] mail parse failed uid=%s error=%s", uid, exc)
                continue

            key = parsed.message_id if parsed.message_id else build_fallback_message_key(parsed)
            if self.store.has(key):
                continue
            unique_fetched_count += 1

            if parsed.sender_addr not in self.settings.sender_allowlist:
                self.store.mark(
                    key,
                    action="ignore",
                    mail_date_iso=(
                        parsed.mail_datetime_utc.isoformat()
                        if parsed.mail_datetime_utc
                        else ""
                    ),
                    retry_count=0,
                )
                self.stats["ignored"] += 1
                continue

            if not in_alert_window(parsed.mail_datetime_utc, now_utc, self.settings.alert_window_hours):
                self.store.mark(
                    key,
                    action="ignore",
                    mail_date_iso=(
                        parsed.mail_datetime_utc.isoformat()
                        if parsed.mail_datetime_utc
                        else ""
                    ),
                    retry_count=0,
                )
                self.stats["ignored"] += 1
                continue

            eligible_items.append((key, parsed))

        self.stats["eligible"] += len(eligible_items)
        process_items = eligible_items

        if self._bootstrap_pending:
            mode = self.settings.bootstrap_mode
            if mode == "silent_index":
                for key, parsed in eligible_items:
                    self.store.mark(
                        key,
                        action="bootstrap",
                        mail_date_iso=(
                            parsed.mail_datetime_utc.isoformat()
                            if parsed.mail_datetime_utc
                            else ""
                        ),
                        retry_count=0,
                    )
                    self.stats["bootstrap_indexed"] += 1
                self.store.cleanup()
                self.store.save()
                self._bootstrap_pending = False
                return unique_fetched_count

            if mode == "latest_one" and len(eligible_items) > 1:
                for key, parsed in eligible_items[:-1]:
                    self.store.mark(
                        key,
                        action="bootstrap",
                        mail_date_iso=(
                            parsed.mail_datetime_utc.isoformat()
                            if parsed.mail_datetime_utc
                            else ""
                        ),
                        retry_count=0,
                    )
                    self.stats["bootstrap_indexed"] += 1
                process_items = [eligible_items[-1]]

            self._bootstrap_pending = False

        for key, parsed in process_items:
            decision, retry_count = await self._classify_with_retry(parsed)
            mail_date_iso = (
                parsed.mail_datetime_utc.isoformat()
                if parsed.mail_datetime_utc
                else ""
            )

            if decision is None:
                self.store.mark(
                    key,
                    action="ignore",
                    mail_date_iso=mail_date_iso,
                    retry_count=retry_count,
                )
                self.stats["ignored"] += 1
                continue

            if decision.is_alert:
                pushed = await self._push_alert(parsed, decision)
                if pushed > 0:
                    self.store.mark(
                        key,
                        action="push",
                        mail_date_iso=mail_date_iso,
                        retry_count=retry_count,
                    )
                    self.stats["pushed"] += 1
                else:
                    self.store.mark(
                        key,
                        action="ignore",
                        mail_date_iso=mail_date_iso,
                        retry_count=retry_count,
                    )
                    self.stats["ignored"] += 1
            else:
                self.store.mark(
                    key,
                    action="ignore",
                    mail_date_iso=mail_date_iso,
                    retry_count=retry_count,
                )
                self.stats["ignored"] += 1

        self.store.cleanup()
        self.store.save()
        return unique_fetched_count

    async def _classify_with_retry(self, parsed_mail) -> tuple[AlertDecision | None, int]:
        last_error = ""
        for attempt in range(1, self.settings.llm_retry_limit + 1):
            try:
                decision = await self.classifier.classify(
                    parsed_mail,
                    max_summary_chars=self.settings.max_summary_chars,
                    llm_provider_id=self.settings.llm_provider_id,
                    prompt_template=self.settings.llm_prompt_template,
                    debug_log=self.settings.debug_log,
                )
                if decision is not None:
                    return decision, attempt - 1
                last_error = "LLM returned invalid JSON payload."
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"

            if attempt < self.settings.llm_retry_limit:
                await asyncio.sleep(min(1.5 * attempt, 5))

        self.stats["llm_failed"] += 1
        self.last_error = last_error or "LLM classification failed."
        return None, self.settings.llm_retry_limit

    async def _push_alert(
        self,
        parsed_mail,
        decision: AlertDecision,
        *,
        target_sessions: list[str] | None = None,
        title: str = "校园安全警报",
        count_stats: bool = True,
    ) -> int:
        sessions = target_sessions if target_sessions is not None else self.settings.target_sessions
        if not sessions:
            self.last_error = "No target sessions configured."
            if count_stats:
                self.stats["push_failed"] += 1
            return 0

        category_label = CATEGORY_LABELS.get(decision.category, decision.category)
        mail_time = (
            parsed_mail.mail_datetime_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
            if parsed_mail.mail_datetime_utc
            else parsed_mail.raw_date
        )
        message = (
            f"{title}\n"
            f"分类: {category_label}\n"
            f"摘要: {decision.summary}\n"
            f"主题: {parsed_mail.subject or '(无主题)'}\n"
            f"时间: {mail_time}\n"
            f"来源: {parsed_mail.sender_addr}\n"
            f"置信度: {decision.confidence:.2f}"
        )

        sent = 0
        chain = MessageChain([Plain(message)])
        for session in sessions:
            try:
                ok = await self.context.send_message(session, chain)
                if ok:
                    sent += 1
                else:
                    if count_stats:
                        self.stats["push_failed"] += 1
                    logger.warning(
                        "[CampusAlert] send_message returned False. target=%s",
                        session,
                    )
            except Exception as exc:
                if count_stats:
                    self.stats["push_failed"] += 1
                logger.error(
                    "[CampusAlert] push failed target=%s error=%s",
                    session,
                    exc,
                )
        return sent

    async def _run_test_flow(self, trigger_session: str) -> tuple[bool, str]:
        async with self._poll_lock:
            self._refresh_settings()
            now_utc = datetime.now(timezone.utc)

            sender_raw = self.settings.test_sender
            subject = self.settings.test_subject or "[TEST] CampusAlert 模拟告警邮件"
            body = self.settings.test_body

            if not sender_raw:
                return False, "测试失败：请先配置 test_sender。"
            if not body:
                return False, "测试失败：请先配置 test_body。"
            if not trigger_session:
                return False, "测试失败：当前会话无效，无法执行测试推送。"

            sender_name, sender_addr = parseaddr(sender_raw)
            sender_addr = sender_addr.strip().lower()
            if not sender_addr:
                sender_addr = sender_raw.strip().lower()
            if "@" not in sender_addr:
                return False, "测试失败：test_sender 不是有效邮箱地址。"

            sender_display = sender_raw.strip()
            if not sender_name and sender_addr == sender_display.lower():
                sender_display = f"CampusAlertTest <{sender_addr}>"

            parsed_mail = ParsedMail(
                uid=f"test-{int(now_utc.timestamp())}",
                message_id=f"<campusalert-test-{int(now_utc.timestamp() * 1000)}>",
                sender=sender_display,
                sender_addr=sender_addr,
                subject=subject,
                raw_date=now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
                mail_datetime_utc=now_utc,
                body_text=body[: self.settings.max_body_chars],
            )

            decision, _ = await self._classify_with_retry(parsed_mail)
            if decision is None:
                return False, "测试失败：LLM 判定失败（返回格式无效或重试耗尽）。"

            category_label = CATEGORY_LABELS.get(decision.category, decision.category)
            if not decision.is_alert:
                return True, (
                    "测试完成：LLM 判定不推送。\n"
                    f"分类：{category_label}\n"
                    f"置信度：{decision.confidence:.2f}\n"
                    f"摘要：{decision.summary or '(空)'}"
                )

            pushed = await self._push_alert(
                parsed_mail,
                decision,
                target_sessions=[trigger_session],
                title="校园安全警报[TEST]",
                count_stats=False,
            )
            if pushed > 0:
                return True, (
                    "测试完成：LLM 判定需要推送，已推送到当前会话。\n"
                    f"分类：{category_label}\n"
                    f"置信度：{decision.confidence:.2f}\n"
                    f"摘要：{decision.summary}"
                )
            return False, "测试失败：LLM 判定需要推送，但发送到当前会话失败。"

    async def _run_manual_check(self) -> tuple[bool, str]:
        try:
            async with self._poll_lock:
                self._refresh_settings()
                if not self.settings.enabled:
                    self.runtime_state = "disabled"
                    return True, "立即检查未执行：插件当前处于关闭状态。"

                if not self.settings.has_required_imap():
                    self.runtime_state = "config_error"
                    self.last_error = "IMAP 配置缺少必填项。"
                    return False, "立即检查失败：IMAP 配置缺少必填项。"

                before = self._snapshot_stats()
                await self._ensure_imap_client()
                fetched_count = await self._poll_once()
                self.stats["cycles"] += 1
                self.stats["fetched"] += fetched_count
                self.last_poll_at = datetime.now(timezone.utc)
                self.last_error = ""
                self.runtime_state = "running"
                self.backoff_seconds = self.settings.network_retry_base_sec
                after = self._snapshot_stats()
        except Exception as exc:
            async with self._poll_lock:
                self.runtime_state = "error"
                self.last_error = f"{type(exc).__name__}: {exc}"
                logger.error("[CampusAlert] Manual check failed: %s", self.last_error)
                await self._close_imap_client()
            return False, f"立即检查失败：{self.last_error}"

        eligible_delta = max(0, after["eligible"] - before["eligible"])
        pushed_delta = max(0, after["pushed"] - before["pushed"])
        ignored_delta = max(0, after["ignored"] - before["ignored"])
        llm_failed_delta = max(0, after["llm_failed"] - before["llm_failed"])
        push_failed_delta = max(0, after["push_failed"] - before["push_failed"])

        lines = [
            "立即检查完成。",
            f"本轮新增邮件（去重后）：{fetched_count} 封",
            f"符合规则邮件：{eligible_delta} 封",
            f"告警推送条数：{pushed_delta} 条",
        ]
        if pushed_delta > 0:
            lines.append(f"结果：发现 {pushed_delta} 条告警并已尝试推送。")
        else:
            lines.append("结果：未发现需要推送的告警。")
        if ignored_delta > 0:
            lines.append(f"忽略邮件：{ignored_delta} 封")
        if llm_failed_delta > 0:
            lines.append(f"LLM 失败：{llm_failed_delta} 封")
        if push_failed_delta > 0:
            lines.append(f"推送失败：{push_failed_delta} 次")
        return True, "\n".join(lines)

    async def _reload_now(self) -> tuple[bool, str]:
        self._refresh_settings()
        try:
            async with self._poll_lock:
                await self._close_imap_client()
                if self.settings.enabled and self.settings.has_required_imap():
                    await self._ensure_imap_client()
                # Reset backoff so manual reload restores normal polling cadence.
                self.backoff_seconds = self.settings.network_retry_base_sec

                if not self.settings.enabled:
                    self.runtime_state = "disabled"
                    self.last_error = ""
                    message = "重载成功：配置已加载，插件当前处于关闭状态。"
                elif not self.settings.has_required_imap():
                    self.runtime_state = "config_error"
                    self.last_error = "IMAP 配置缺少必填项。"
                    message = "重载成功：配置已加载，但 IMAP 必填项不完整。"
                else:
                    self.runtime_state = "idle"
                    self.last_error = ""
                    message = "重载成功：配置已加载并重建 IMAP 连接。"
        except Exception as exc:
            async with self._poll_lock:
                self.runtime_state = "error"
                self.last_error = f"{type(exc).__name__}: {exc}"
                await self._close_imap_client()
            logger.error("[CampusAlert] reload failed: %s", self.last_error)
            return False, f"重载失败：{self.last_error}"

        logger.info("[CampusAlert] %s", message)
        return True, message

    async def _build_status_text(self) -> str:
        async with self._poll_lock:
            runtime_state = self.runtime_state
            enabled = self.settings.enabled
            last_poll = self._format_dt_utc(self.last_poll_at)
            records = self.store.count()
            stats = self._snapshot_stats()
            last_error = self.last_error or "无"
        return (
            "校园告警插件状态\n"
            f"运行状态：{self._state_text(runtime_state)}\n"
            f"插件开关：{'开启' if enabled else '关闭'}\n"
            f"最近轮询时间：{last_poll}\n"
            f"去重记录数：{records}\n"
            f"轮询次数：{stats['cycles']}\n"
            f"拉取邮件总数：{stats['fetched']}\n"
            f"符合规则总数：{stats['eligible']}\n"
            f"已推送告警：{stats['pushed']}\n"
            f"已忽略邮件：{stats['ignored']}\n"
            f"启动静默索引：{stats['bootstrap_indexed']}\n"
            f"LLM 失败次数：{stats['llm_failed']}\n"
            f"推送失败次数：{stats['push_failed']}\n"
            f"最近错误：{last_error}"
        )

    @filter.command("campusalert")
    async def campusalert_command(self, event: AstrMessageEvent):
        """校园告警插件运维指令: /campusalert status|checknow|reload|test"""
        tokens = [item for item in self.parse_commands(event.message_str).tokens if item]
        subcommand = "status"
        settings = self._refresh_settings()

        for idx, token in enumerate(tokens):
            normalized = token.lstrip("/").lower()
            if normalized == "campusalert":
                if idx + 1 < len(tokens):
                    subcommand = tokens[idx + 1].strip().lower()
                break

        if settings.command_admin_only and not event.is_admin():
            logger.info(
                "[CampusAlert] 拒绝非管理员指令。发送者=%s，会话=%s",
                event.get_sender_id(),
                event.unified_msg_origin,
            )
            yield event.plain_result("仅 AstrBot 管理员可使用该指令。")
            return

        if subcommand == "status":
            yield event.plain_result(await self._build_status_text())
            return

        if subcommand == "checknow":
            _, message = await self._run_manual_check()
            yield event.plain_result(message)
            return

        if subcommand == "reload":
            _, message = await self._reload_now()
            yield event.plain_result(message)
            return

        if subcommand == "test":
            _, message = await self._run_test_flow(event.unified_msg_origin)
            yield event.plain_result(message)
            return

        yield event.plain_result(
            "用法: /campusalert status | /campusalert checknow | /campusalert reload | /campusalert test",
        )
