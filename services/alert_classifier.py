from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from astrbot.api import logger
from astrbot.api.star import Context

from .mail_parser import ParsedMail


ALLOWED_CATEGORIES = {
    "security_alert",
    "class_cancel",
    "public_health",
    "other",
}


DEFAULT_PROMPT_TEMPLATE = (
"""
你是校园预警邮件分类助手。你必须只输出 JSON，不要输出其他文字。
请基于邮件内容判断是否需要推送校园预警通知。

判定标准：
1) security_alert: 人身/财产/治安/暴力/紧急疏散等。
2) class_cancel: 停课、停考、校区关闭、教学活动取消。
3) public_health: 传染病、饮水或食物安全、公共卫生风险。
4) other: 不需要推送。

输出 JSON 结构固定为：
{{"is_alert": bool, "category": "security_alert|class_cancel|public_health|other", "summary": "string", "confidence": 0.0}}

summary 撰写严格要求：
1. 必须包含：[时间+地点+事件]、[关键例外/特殊安排]（如网课是否照常、特定建筑是否开放）、[后续信息获取渠道/更新时间]。
2. 严禁包含：禁止添加任何邮件原文中未提及的“安全建议”、“温馨提示”或客套话（如“请注意安全”等）。
3. 语言风格：专业、客观、精炼。
4. 字数限制：必须使用中文，如果是英文的建筑名称可以维持英文名称不变，不超过 {max_summary_chars} 个中文字符。
5. 如果 is_alert 为 false，summary 可为空字符串。

邮件信息：
From: {sender}
Date(UTC): {mail_date_utc}
Subject: {subject}
Body:
{body}
"""
)


@dataclass
class AlertDecision:
    is_alert: bool
    category: str
    summary: str
    confidence: float
    raw_text: str


def _extract_first_json_object(text: str) -> str | None:
    start = text.find("{")
    if start < 0:
        return None

    depth = 0
    in_string = False
    escape = False
    for idx, ch in enumerate(text[start:], start=start):
        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : idx + 1]
    return None


def _parse_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    return None


def _clamp_confidence(value: Any) -> float:
    try:
        confidence = float(value)
    except Exception:
        return 0.0
    if confidence < 0:
        return 0.0
    if confidence > 1:
        return 1.0
    return confidence


def _normalize_summary(value: Any, max_summary_chars: int) -> str:
    if not isinstance(value, str):
        return ""
    summary = " ".join(value.split()).strip()
    max_len = max(1, int(max_summary_chars))
    if len(summary) > max_len:
        summary = summary[:max_len]
    return summary


def _build_decision(
    payload: dict[str, Any],
    *,
    max_summary_chars: int,
    raw_text: str,
) -> AlertDecision | None:
    is_alert = _parse_bool(payload.get("is_alert"))
    if is_alert is None:
        return None

    category = str(payload.get("category", "other")).strip()
    if category not in ALLOWED_CATEGORIES:
        category = "other"

    summary = _normalize_summary(payload.get("summary", ""), max_summary_chars)
    confidence = _clamp_confidence(payload.get("confidence", 0.0))

    if is_alert and not summary:
        return None

    return AlertDecision(
        is_alert=is_alert,
        category=category,
        summary=summary,
        confidence=confidence,
        raw_text=raw_text,
    )


class AlertClassifier:
    def __init__(self, context: Context) -> None:
        self.context = context

    async def classify(
        self,
        parsed_mail: ParsedMail,
        *,
        max_summary_chars: int,
        llm_provider_id: str = "",
        prompt_template: str | None = None,
        debug_log: bool = False,
    ) -> AlertDecision | None:
        template = prompt_template.strip() if prompt_template else DEFAULT_PROMPT_TEMPLATE
        format_data = {
            "sender": parsed_mail.sender or parsed_mail.sender_addr,
            "mail_date_utc": (
                parsed_mail.mail_datetime_utc.isoformat()
                if parsed_mail.mail_datetime_utc
                else ""
            ),
            "subject": parsed_mail.subject or "",
            "body": parsed_mail.body_text or "",
            "max_summary_chars": max_summary_chars,
        }
        try:
            prompt = template.format(**format_data)
        except Exception:
            prompt = DEFAULT_PROMPT_TEMPLATE.format(**format_data)

        if debug_log:
            logger.debug(
                "[CampusAlert] LLM 输入：from=%s, date_utc=%s, subject=%s, body_chars=%s, prompt_chars=%s",
                format_data["sender"],
                format_data["mail_date_utc"],
                format_data["subject"] or "(无主题)",
                len(format_data["body"]),
                len(prompt),
            )
            logger.debug(
                "[CampusAlert] LLM 输入 Prompt(截断): %s",
                prompt[:1200],
            )

        provider_id = (llm_provider_id or "").strip()
        if not provider_id:
            provider = self.context.get_using_provider()
            provider_id = provider.meta().id

        if debug_log:
            logger.debug("[CampusAlert] LLM 使用 Provider ID: %s", provider_id)

        llm_response = await self.context.llm_generate(
            chat_provider_id=provider_id,
            prompt=prompt,
            temperature=0.1,
        )
        raw_text = (llm_response.completion_text or "").strip()
        if debug_log:
            logger.debug(
                "[CampusAlert] LLM 返回原文(截断): %s",
                raw_text[:800],
            )
        decision = self.parse_decision(raw_text, max_summary_chars=max_summary_chars)
        if debug_log:
            if decision is None:
                logger.debug("[CampusAlert] LLM 解析结果：无效 JSON 或校验失败")
            else:
                logger.debug(
                    "[CampusAlert] LLM 解析结果：is_alert=%s, category=%s, confidence=%.2f, summary=%s",
                    decision.is_alert,
                    decision.category,
                    decision.confidence,
                    decision.summary,
                )
        return decision

    @staticmethod
    def parse_decision(raw_text: str, *, max_summary_chars: int) -> AlertDecision | None:
        candidates = [raw_text]
        extracted = _extract_first_json_object(raw_text)
        if extracted and extracted not in candidates:
            candidates.append(extracted)

        for candidate in candidates:
            try:
                payload = json.loads(candidate)
            except Exception:
                continue
            if isinstance(payload, dict):
                decision = _build_decision(
                    payload,
                    max_summary_chars=max_summary_chars,
                    raw_text=raw_text,
                )
                if decision:
                    return decision
        return None
