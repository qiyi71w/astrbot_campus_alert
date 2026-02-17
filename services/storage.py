from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


class ProcessedEmailStore:
    """Persistent dedup store for processed email identifiers."""

    def __init__(
        self,
        data_dir: Path,
        file_name: str = "processed_emails.json",
        retention_days: int = 30,
        max_items: int = 10000,
    ) -> None:
        self.data_dir = data_dir
        self.file_path = data_dir / file_name
        self.retention_days = max(1, int(retention_days))
        self.max_items = max(100, int(max_items))
        self.records: dict[str, dict[str, Any]] = {}
        self.first_bootstrap = False
        self._dirty = False

    def load(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.first_bootstrap = not self.file_path.exists()
        if not self.file_path.exists():
            self.records = {}
            self._dirty = True
            self.save(force=True)
            return

        try:
            raw = self.file_path.read_text(encoding="utf-8")
            payload = json.loads(raw) if raw.strip() else {}
            records = payload.get("records", {})
            if not isinstance(records, dict):
                records = {}
            self.records = {
                str(k): v for k, v in records.items() if isinstance(v, dict)
            }
        except Exception:
            backup_path = self.file_path.with_suffix(".corrupt.json")
            try:
                self.file_path.replace(backup_path)
            except OSError:
                pass
            self.records = {}
            self.first_bootstrap = True
            self._dirty = True
            self.save(force=True)
            return

        self.cleanup()
        self._dirty = False

    def has(self, key: str) -> bool:
        return key in self.records

    def mark(
        self,
        key: str,
        *,
        action: str,
        mail_date_iso: str | None,
        retry_count: int = 0,
    ) -> None:
        entry = {
            "processed_at": _now_utc_iso(),
            "mail_date": mail_date_iso or "",
            "action": action,
            "retry_count": max(0, int(retry_count)),
        }
        if key in self.records:
            self.records.pop(key, None)
        self.records[key] = entry
        self._dirty = True

    def cleanup(self) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.retention_days)
        removable: list[str] = []
        for key, item in self.records.items():
            processed_at = _parse_iso_datetime(item.get("processed_at"))
            if processed_at is None:
                removable.append(key)
                continue
            if processed_at < cutoff:
                removable.append(key)
        for key in removable:
            self.records.pop(key, None)

        overflow = len(self.records) - self.max_items
        if overflow > 0:
            keys = list(self.records.keys())[:overflow]
            for key in keys:
                self.records.pop(key, None)

        if removable or overflow > 0:
            self._dirty = True

    def save(self, force: bool = False) -> None:
        if not force and not self._dirty:
            return
        payload = {
            "version": 1,
            "updated_at": _now_utc_iso(),
            "records": self.records,
        }
        self.data_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = self.file_path.with_suffix(".tmp")
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp_path.replace(self.file_path)
        self._dirty = False

    def count(self) -> int:
        return len(self.records)

