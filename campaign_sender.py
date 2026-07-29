from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
import os
import random
import re
import signal
import sqlite3
import tempfile
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, replace
from email.utils import parseaddr
from enum import StrEnum
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from email_validator import EmailNotValidError, validate_email

LOGGER = logging.getLogger("campaign_sender")

BATCH_SIZE = 50
IMPORT_CHUNK_SIZE = 400
MAX_APPLICATION_ATTEMPTS = 3
MAX_WORKERS = 64
DEFAULT_WORKERS = 8
DEFAULT_TARGET_UTILIZATION = 0.90
REQUIRED_EVENTS = {
    "BOUNCE",
    "COMPLAINT",
    "DELIVERY",
    "REJECT",
    "RENDERING_FAILURE",
}
COMPLETED_STATUSES = {
    "accepted",
    "bad_row",
    "suppressed",
    "permanent_failure",
}
INCOMPLETE_STATUSES = {
    "pending",
    "retryable",
    "in_flight",
    "retry_exhausted",
    "unknown",
}
RETRYABLE_ENTRY_STATUSES = {
    "ACCOUNT_THROTTLED",
    "TRANSIENT_FAILURE",
    "FAILED",
}
GLOBAL_STOP_ENTRY_STATUSES = {
    "ACCOUNT_DAILY_QUOTA_EXCEEDED",
    "ACCOUNT_SUSPENDED",
    "ACCOUNT_SENDING_PAUSED",
    "CONFIGURATION_SET_SENDING_PAUSED",
    "CONFIGURATION_SET_NOT_FOUND",
    "TEMPLATE_NOT_FOUND",
    "MAIL_FROM_DOMAIN_NOT_VERIFIED",
}
PERMANENT_ENTRY_STATUSES = {
    "INVALID_PARAMETER",
    "MESSAGE_REJECTED",
    "INVALID_SENDING_POOL_NAME",
}
RETRYABLE_CLIENT_CODES = {
    "ThrottlingException",
    "TooManyRequestsException",
    "LimitExceededException",
}
GLOBAL_STOP_CLIENT_CODES = {
    "AccountSuspendedException",
    "MailFromDomainNotVerifiedException",
    "NotFoundException",
    "SendingPausedException",
}


class CampaignError(Exception):
    """An expected, operator-actionable campaign error."""


class PreflightError(CampaignError):
    """An SES prerequisite failed."""


class RecipientStatus(StrEnum):
    PENDING = "pending"
    IN_FLIGHT = "in_flight"
    RETRYABLE = "retryable"
    RETRY_EXHAUSTED = "retry_exhausted"
    ACCEPTED = "accepted"
    BAD_ROW = "bad_row"
    PERMANENT_FAILURE = "permanent_failure"
    UNKNOWN = "unknown"
    SUPPRESSED = "suppressed"


@dataclass(frozen=True)
class Settings:
    input_path: Path
    campaign_id: str
    region: str
    from_email: str
    template_name: str
    configuration_set: str
    state_path: Path
    workers: int = DEFAULT_WORKERS
    target_utilization: float = DEFAULT_TARGET_UTILIZATION
    send: bool = False
    confirm_opted_in: bool = False
    retry_exhausted: bool = False
    retry_unknown: bool = False
    accept_duplicate_risk: bool = False

    @property
    def campaign_tag(self) -> str:
        return re.sub(r"[^A-Za-z0-9_-]", "_", self.campaign_id)[:256]


@dataclass(frozen=True)
class Recipient:
    email: str
    name: str
    attempts: int = 0


@dataclass(frozen=True)
class EntryOutcome:
    email: str
    status: RecipientStatus
    attempts: int
    message_id: str | None = None
    reason_code: str | None = None
    detail: str | None = None


@dataclass(frozen=True)
class BatchOutcome:
    entries: tuple[EntryOutcome, ...]
    global_stop: str | None = None


@dataclass(frozen=True)
class AccountLimits:
    max_24_hour_send: float
    sent_last_24_hours: float
    max_send_rate: float


@dataclass(frozen=True)
class PreflightResult:
    limits: AccountLimits
    send_budget: int
    target_rate: float


@dataclass
class ImportStats:
    rows: int = 0
    valid_rows: int = 0
    unique_recipients: int = 0
    bad_rows: int = 0
    duplicate_rows: int = 0


@dataclass(frozen=True)
class SchedulerResult:
    stop_reason: str | None
    scheduled_recipients: int
    interrupted: bool
    max_outstanding: int


SCHEMA = """
CREATE TABLE IF NOT EXISTS campaign (
    campaign_id TEXT PRIMARY KEY,
    input_path TEXT NOT NULL,
    input_sha256 TEXT NOT NULL,
    region TEXT NOT NULL,
    from_email TEXT NOT NULL,
    template_name TEXT NOT NULL,
    configuration_set TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recipient (
    campaign_id TEXT NOT NULL,
    email TEXT NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    message_id TEXT,
    last_reason_code TEXT,
    last_detail TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (campaign_id, email)
);

CREATE TABLE IF NOT EXISTS input_issue (
    campaign_id TEXT NOT NULL,
    row_number INTEGER NOT NULL,
    raw_email TEXT,
    status TEXT NOT NULL DEFAULT 'bad_row',
    reason_code TEXT NOT NULL,
    detail TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (campaign_id, row_number)
);

CREATE INDEX IF NOT EXISTS recipient_work
    ON recipient(campaign_id, status, email);
"""


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def positive_worker_count(value: str) -> int:
    workers = int(value)
    if not 1 <= workers <= MAX_WORKERS:
        raise argparse.ArgumentTypeError(f"workers must be between 1 and {MAX_WORKERS}")
    return workers


def utilization(value: str) -> float:
    parsed = float(value)
    if not 0 < parsed <= DEFAULT_TARGET_UTILIZATION:
        raise argparse.ArgumentTypeError(
            f"target utilization must be > 0 and <= {DEFAULT_TARGET_UTILIZATION}"
        )
    return parsed


def campaign_id(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value):
        raise argparse.ArgumentTypeError(
            "campaign ID must be 1-128 safe filename/tag characters"
        )
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Send a personalized, quota-aware Amazon SES bulk campaign."
    )
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--campaign-id", required=True, type=campaign_id)
    parser.add_argument("--region", required=True)
    parser.add_argument("--from-email", required=True)
    parser.add_argument("--template-name", required=True)
    parser.add_argument("--configuration-set", required=True)
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument(
        "--workers", type=positive_worker_count, default=DEFAULT_WORKERS
    )
    parser.add_argument(
        "--target-utilization",
        type=utilization,
        default=DEFAULT_TARGET_UTILIZATION,
    )
    parser.add_argument("--send", action="store_true")
    parser.add_argument("--confirm-opted-in", action="store_true")
    parser.add_argument("--retry-exhausted", action="store_true")
    parser.add_argument("--retry-unknown", action="store_true")
    parser.add_argument("--accept-duplicate-risk", action="store_true")
    return parser


def parse_settings(argv: Sequence[str] | None = None) -> Settings:
    args = build_parser().parse_args(argv)
    settings = Settings(
        input_path=args.input.expanduser().resolve(),
        campaign_id=args.campaign_id,
        region=args.region,
        from_email=args.from_email,
        template_name=args.template_name,
        configuration_set=args.configuration_set,
        state_path=args.state.expanduser().resolve(),
        workers=args.workers,
        target_utilization=args.target_utilization,
        send=args.send,
        confirm_opted_in=args.confirm_opted_in,
        retry_exhausted=args.retry_exhausted,
        retry_unknown=args.retry_unknown,
        accept_duplicate_risk=args.accept_duplicate_risk,
    )
    validate_safety_flags(settings)
    return settings


def validate_safety_flags(settings: Settings) -> None:
    if settings.send and not settings.confirm_opted_in:
        raise CampaignError("--send requires --confirm-opted-in")
    if (settings.retry_exhausted or settings.retry_unknown) and not settings.send:
        raise CampaignError("recovery flags require --send and --confirm-opted-in")
    if settings.retry_unknown and not settings.accept_duplicate_risk:
        raise CampaignError("--retry-unknown requires --accept-duplicate-risk")
    if settings.accept_duplicate_risk and not settings.retry_unknown:
        raise CampaignError("--accept-duplicate-risk requires --retry-unknown")


def connect_state(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    os.chmod(path, 0o600)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.executescript(SCHEMA)
    return connection


def normalized_email(raw_email: str) -> str:
    if any(ord(character) < 32 or ord(character) == 127 for character in raw_email):
        raise EmailNotValidError("email contains control characters")
    validated = validate_email(
        raw_email.strip(),
        check_deliverability=False,
        allow_smtputf8=False,
    )
    normalized = validated.ascii_email or validated.normalized
    return normalized.lower()


def sanitize_detail(detail: object, email: str | None = None) -> str:
    text = " ".join(str(detail).replace("\x00", "").split())
    if email:
        text = text.replace(email, "<redacted-address>")
    return text[:500]


def _decoded_hashed_lines(handle: Any, digest: Any) -> Iterator[str]:
    first = True
    for raw_line in handle:
        digest.update(raw_line)
        encoding = "utf-8-sig" if first else "utf-8"
        first = False
        yield raw_line.decode(encoding)


def _record_input_issue(
    connection: sqlite3.Connection,
    campaign: str,
    row_number: int,
    raw_email: str | None,
    reason_code: str,
    detail: str,
    *,
    status: str = "bad_row",
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO input_issue (
            campaign_id, row_number, raw_email, status,
            reason_code, detail, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            campaign,
            row_number,
            raw_email,
            status,
            reason_code,
            sanitize_detail(detail, raw_email),
            utc_now(),
        ),
    )


def _flush_recipient_chunk(
    connection: sqlite3.Connection,
    campaign: str,
    rows: list[tuple[int, str, str]],
) -> None:
    if not rows:
        return
    now = utc_now()
    emails = sorted({email for _, email, _ in rows})
    placeholders = ",".join("?" for _ in emails)
    stored = {
        row["email"]: (row["name"], row["status"])
        for row in connection.execute(
            f"""
            SELECT email, name, status
            FROM recipient
            WHERE campaign_id = ? AND email IN ({placeholders})
            """,
            [campaign, *emails],
        )
    }
    new_rows: list[tuple[int, str, str]] = []
    conflicts: list[tuple[int, str, str]] = []
    for row_number, email, name in rows:
        previous = stored.get(email)
        if previous is None:
            stored[email] = (name, RecipientStatus.PENDING)
            new_rows.append((row_number, email, name))
        elif name == previous[0]:
            _record_input_issue(
                connection,
                campaign,
                row_number,
                email,
                "duplicate_identical",
                "Duplicate normalized email and name",
                status="duplicate",
            )
        else:
            conflicts.append((row_number, email, name))

    connection.executemany(
        """
        INSERT OR IGNORE INTO recipient (
            campaign_id, email, name, status, attempts, updated_at
        ) VALUES (?, ?, ?, 'pending', 0, ?)
        """,
        [(campaign, email, name, now) for _, email, name in new_rows],
    )
    for row_number, email, _name in conflicts:
        _stored_name, stored_status = stored[email]
        if stored_status != RecipientStatus.BAD_ROW:
            connection.execute(
                """
                UPDATE recipient
                SET status = 'bad_row',
                    last_reason_code = 'duplicate_conflict',
                    last_detail = 'Normalized address has conflicting names',
                    updated_at = ?
                WHERE campaign_id = ? AND email = ?
                """,
                (now, campaign, email),
            )
            stored[email] = (_stored_name, RecipientStatus.BAD_ROW)
        _record_input_issue(
            connection,
            campaign,
            row_number,
            email,
            "duplicate_conflict",
            "Normalized address has conflicting names",
        )


def import_recipients(
    csv_path: Path,
    connection: sqlite3.Connection,
    settings: Settings,
) -> ImportStats:
    if not csv_path.is_file():
        raise CampaignError("input CSV does not exist or is not a regular file")

    stats = ImportStats()
    digest = hashlib.sha256()
    chunk: list[tuple[int, str, str]] = []
    now = utc_now()
    try:
        with connection, csv_path.open("rb") as binary_file:
            connection.execute(
                """
                INSERT INTO campaign (
                    campaign_id, input_path, input_sha256, region, from_email,
                    template_name, configuration_set, created_at
                ) VALUES (?, ?, '', ?, ?, ?, ?, ?)
                """,
                (
                    settings.campaign_id,
                    str(csv_path),
                    settings.region,
                    settings.from_email,
                    settings.template_name,
                    settings.configuration_set,
                    now,
                ),
            )
            reader = csv.DictReader(_decoded_hashed_lines(binary_file, digest))
            if reader.fieldnames is None or set(reader.fieldnames) != {"name", "email"}:
                raise CampaignError("input CSV must contain exactly name,email headers")

            for row_number, row in enumerate(reader, start=2):
                stats.rows += 1
                raw_email = (row.get("email") or "").strip()
                name = (row.get("name") or "").strip()
                if not raw_email:
                    _record_input_issue(
                        connection,
                        settings.campaign_id,
                        row_number,
                        None,
                        "missing_email",
                        "Email is required",
                    )
                    continue
                if not name:
                    _record_input_issue(
                        connection,
                        settings.campaign_id,
                        row_number,
                        raw_email,
                        "missing_name",
                        "Name is required",
                    )
                    continue
                try:
                    email = normalized_email(raw_email)
                except EmailNotValidError as error:
                    _record_input_issue(
                        connection,
                        settings.campaign_id,
                        row_number,
                        raw_email,
                        "invalid_email",
                        str(error),
                    )
                    continue
                stats.valid_rows += 1
                chunk.append((row_number, email, name))
                if len(chunk) >= IMPORT_CHUNK_SIZE:
                    _flush_recipient_chunk(connection, settings.campaign_id, chunk)
                    chunk.clear()
            _flush_recipient_chunk(connection, settings.campaign_id, chunk)
            connection.execute(
                "UPDATE campaign SET input_sha256 = ? WHERE campaign_id = ?",
                (digest.hexdigest(), settings.campaign_id),
            )
    except UnicodeDecodeError as error:
        raise CampaignError("input CSV must be UTF-8 encoded") from error
    except csv.Error as error:
        raise CampaignError(f"input CSV is malformed: {error}") from error

    stats.unique_recipients = int(
        connection.execute(
            "SELECT COUNT(*) FROM recipient WHERE campaign_id = ?",
            (settings.campaign_id,),
        ).fetchone()[0]
    )
    input_issues = int(
        connection.execute(
            """
            SELECT COUNT(*) FROM input_issue
            WHERE campaign_id = ? AND status = 'bad_row'
            """,
            (settings.campaign_id,),
        ).fetchone()[0]
    )
    stats.duplicate_rows = int(
        connection.execute(
            """
            SELECT COUNT(*) FROM input_issue
            WHERE campaign_id = ? AND status = 'duplicate'
            """,
            (settings.campaign_id,),
        ).fetchone()[0]
    )
    conflict_recipients = int(
        connection.execute(
            """
            SELECT COUNT(*) FROM recipient
            WHERE campaign_id = ? AND status = 'bad_row'
            """,
            (settings.campaign_id,),
        ).fetchone()[0]
    )
    stats.bad_rows = input_issues + conflict_recipients
    return stats


def resolve_campaign(
    connection: sqlite3.Connection, requested: Settings
) -> tuple[Settings, ImportStats | None]:
    row = connection.execute(
        "SELECT * FROM campaign WHERE campaign_id = ?",
        (requested.campaign_id,),
    ).fetchone()
    if row is None:
        stats = import_recipients(requested.input_path, connection, requested)
        return requested, stats

    stored_values = {
        "input": Path(row["input_path"]),
        "region": row["region"],
        "from-email": row["from_email"],
        "template-name": row["template_name"],
        "configuration-set": row["configuration_set"],
    }
    requested_values = {
        "input": requested.input_path,
        "region": requested.region,
        "from-email": requested.from_email,
        "template-name": requested.template_name,
        "configuration-set": requested.configuration_set,
    }
    changed = sorted(
        name
        for name, stored_value in stored_values.items()
        if requested_values[name] != stored_value
    )
    if changed:
        LOGGER.warning(
            "Ignoring changed bootstrap arguments; "
            "stored campaign is authoritative: %s",
            ", ".join(changed),
        )
    effective = replace(
        requested,
        input_path=Path(row["input_path"]),
        region=row["region"],
        from_email=row["from_email"],
        template_name=row["template_name"],
        configuration_set=row["configuration_set"],
    )
    return effective, None


def reset_stale_in_flight(connection: sqlite3.Connection, campaign: str) -> int:
    with connection:
        cursor = connection.execute(
            """
            UPDATE recipient
            SET status = 'unknown',
                last_reason_code = 'interrupted_in_flight',
                last_detail = 'Previous process ended before persisting a response',
                updated_at = ?
            WHERE campaign_id = ? AND status = 'in_flight'
            """,
            (utc_now(), campaign),
        )
    return cursor.rowcount


def apply_recovery_actions(connection: sqlite3.Connection, settings: Settings) -> None:
    with connection:
        if settings.retry_exhausted:
            connection.execute(
                """
                UPDATE recipient
                SET status = 'pending', attempts = 0, message_id = NULL,
                    last_reason_code = NULL, last_detail = NULL, updated_at = ?
                WHERE campaign_id = ? AND status = 'retry_exhausted'
                """,
                (utc_now(), settings.campaign_id),
            )
        if settings.retry_unknown:
            connection.execute(
                """
                UPDATE recipient
                SET status = 'pending', attempts = 0, message_id = NULL,
                    last_reason_code = NULL, last_detail = NULL, updated_at = ?
                WHERE campaign_id = ? AND status = 'unknown'
                """,
                (utc_now(), settings.campaign_id),
            )


def create_ses_client(settings: Settings) -> Any:
    config = Config(
        retries={"mode": "standard", "total_max_attempts": 1},
        connect_timeout=10,
        read_timeout=60,
        tcp_keepalive=True,
        max_pool_connections=max(settings.workers, 10),
    )
    return boto3.client("sesv2", region_name=settings.region, config=config)


def _chunks(
    values: Sequence[str], size: int = IMPORT_CHUNK_SIZE
) -> Iterator[list[str]]:
    for start in range(0, len(values), size):
        yield list(values[start : start + size])


def mark_suppressed_destinations(
    connection: sqlite3.Connection, client: Any, campaign: str
) -> int:
    changed = 0
    next_token: str | None = None
    while True:
        request = {"NextToken": next_token} if next_token else {}
        page = client.list_suppressed_destinations(**request)
        addresses = [
            str(item["EmailAddress"]).lower()
            for item in page.get("SuppressedDestinationSummaries", [])
            if item.get("EmailAddress")
        ]
        for address_chunk in _chunks(addresses):
            placeholders = ",".join("?" for _ in address_chunk)
            with connection:
                cursor = connection.execute(
                    f"""
                    UPDATE recipient
                    SET status = 'suppressed',
                        last_reason_code = 'account_suppression_list',
                        last_detail = 'Address is present in the SES suppression list',
                        updated_at = ?
                    WHERE campaign_id = ?
                      AND status IN ('pending', 'retryable')
                      AND email IN ({placeholders})
                    """,
                    [utc_now(), campaign, *address_chunk],
                )
            changed += cursor.rowcount
        next_token = page.get("NextToken")
        if not next_token:
            break
    return changed


def _require_account_health(account: dict[str, Any]) -> None:
    if not account.get("ProductionAccessEnabled"):
        raise PreflightError("SES production access is required")
    if not account.get("SendingEnabled"):
        raise PreflightError("SES account sending is disabled")
    if account.get("EnforcementStatus") != "HEALTHY":
        raise PreflightError("SES account enforcement status is not HEALTHY")
    suppression = set(
        account.get("SuppressionAttributes", {}).get("SuppressedReasons", [])
    )
    missing = {"BOUNCE", "COMPLAINT"} - suppression
    if missing:
        raise PreflightError(
            "SES account suppression must include BOUNCE and COMPLAINT"
        )


def _verify_template(client: Any, template_name: str) -> None:
    response = client.get_email_template(TemplateName=template_name)
    content = response.get("TemplateContent", {})
    searchable = "\n".join(
        str(content.get(key, "")) for key in ("Subject", "Text", "Html")
    )
    if "{{name}}" not in searchable:
        raise PreflightError("stored SES template must contain {{name}}")
    if not content.get("Text") or not content.get("Html"):
        raise PreflightError("stored SES template must contain text and HTML content")
    client.test_render_email_template(
        TemplateName=template_name,
        TemplateData=json.dumps({"name": "Customer"}),
    )


def _verify_configuration_set(client: Any, configuration_set: str) -> None:
    client.get_configuration_set(ConfigurationSetName=configuration_set)
    response = client.get_configuration_set_event_destinations(
        ConfigurationSetName=configuration_set
    )
    covered: set[str] = set()
    for destination in response.get("EventDestinations", []):
        if destination.get("Enabled"):
            covered.update(destination.get("MatchingEventTypes", []))
    missing = REQUIRED_EVENTS - covered
    if missing:
        raise PreflightError(
            "configuration set event publishing is missing: "
            + ", ".join(sorted(missing))
        )


def preflight(
    client: Any,
    pending_count: int,
    settings: Settings,
) -> PreflightResult:
    account = client.get_account()
    _require_account_health(account)
    quota = account.get("SendQuota", {})
    max_rate = float(quota.get("MaxSendRate", 0))
    max_24 = float(quota.get("Max24HourSend", 0))
    sent_24 = float(quota.get("SentLast24Hours", 0))
    if max_rate <= 0:
        raise PreflightError("SES MaxSendRate must be greater than zero")

    _display_name, identity_address = parseaddr(settings.from_email)
    if not identity_address:
        raise PreflightError("From email address is invalid")
    identity = client.get_email_identity(EmailIdentity=identity_address)
    if not identity.get("VerifiedForSendingStatus"):
        raise PreflightError("From identity is not verified for sending")
    _verify_template(client, settings.template_name)
    _verify_configuration_set(client, settings.configuration_set)

    if max_24 == -1:
        send_budget = pending_count
    else:
        send_budget = max(0, min(pending_count, math.floor(max_24 - sent_24)))
    return PreflightResult(
        limits=AccountLimits(max_24, sent_24, max_rate),
        send_budget=send_budget,
        target_rate=settings.target_utilization * max_rate,
    )


def build_bulk_request(
    batch: Sequence[Recipient], settings: Settings
) -> dict[str, Any]:
    if not 1 <= len(batch) <= BATCH_SIZE:
        raise ValueError(f"batch must contain 1-{BATCH_SIZE} recipients")
    return {
        "FromEmailAddress": settings.from_email,
        "DefaultContent": {
            "Template": {
                "TemplateName": settings.template_name,
                "TemplateData": json.dumps({"name": "Customer"}),
            }
        },
        "BulkEmailEntries": [
            {
                "Destination": {"ToAddresses": [recipient.email]},
                "ReplacementEmailContent": {
                    "ReplacementTemplate": {
                        "ReplacementTemplateData": json.dumps(
                            {"name": recipient.name},
                            ensure_ascii=False,
                        )
                    }
                },
                "ReplacementTags": [
                    {"Name": "campaign", "Value": settings.campaign_tag}
                ],
            }
            for recipient in batch
        ],
        "ConfigurationSetName": settings.configuration_set,
    }


def planned_batch_sizes(
    recipient_count: int,
    send_budget: int | None = None,
) -> Iterator[int]:
    remaining = min(
        recipient_count,
        recipient_count if send_budget is None else send_budget,
    )
    while remaining > 0:
        size = min(BATCH_SIZE, remaining)
        yield size
        remaining -= size


class RecipientRateLimiter:
    def __init__(
        self,
        rate: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        self.base_rate = rate
        self.current_rate = rate
        self.capacity = max(float(BATCH_SIZE), rate)
        self.tokens = self.capacity
        self.updated_at = clock()
        self.clock = clock
        self.sleeper = sleeper
        self.lock = threading.Lock()
        self.successful_batches = 0

    def acquire(self, tokens: int) -> None:
        if tokens <= 0 or tokens > BATCH_SIZE:
            raise ValueError("recipient tokens must be between 1 and 50")
        while True:
            with self.lock:
                now = self.clock()
                elapsed = max(0.0, now - self.updated_at)
                self.tokens = min(
                    self.capacity,
                    self.tokens + elapsed * self.current_rate,
                )
                self.updated_at = now
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return
                delay = (tokens - self.tokens) / self.current_rate
            self.sleeper(delay)

    def record_throttle(self) -> None:
        with self.lock:
            self.current_rate = max(1.0, self.current_rate * 0.8)
            self.tokens = 0.0
            self.successful_batches = 0

    def record_success(self) -> None:
        with self.lock:
            self.successful_batches += 1
            if self.successful_batches >= 10 and self.current_rate < self.base_rate:
                self.current_rate = min(self.base_rate, self.current_rate * 1.05)
                self.successful_batches = 0


def full_jitter_delay(attempts: int, rng: random.Random) -> float:
    cap = min(60.0, float(2 ** max(0, attempts - 1)))
    return rng.uniform(0.0, cap)


def _client_error_code(error: ClientError) -> str:
    return str(error.response.get("Error", {}).get("Code", "client_error"))


def _unknown_outcomes(
    batch: Sequence[Recipient],
    attempts: dict[str, int],
    reason_code: str,
    detail: str,
) -> tuple[EntryOutcome, ...]:
    return tuple(
        EntryOutcome(
            recipient.email,
            RecipientStatus.UNKNOWN,
            attempts[recipient.email],
            reason_code=reason_code,
            detail=sanitize_detail(detail, recipient.email),
        )
        for recipient in batch
    )


def send_batch(
    client: Any,
    settings: Settings,
    batch: Sequence[Recipient],
    limiter: RecipientRateLimiter,
    *,
    rng: random.Random | None = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> BatchOutcome:
    random_source = rng or random.Random()
    remaining = list(batch)
    outcomes: list[EntryOutcome] = []
    attempts = {recipient.email: recipient.attempts for recipient in batch}

    while remaining:
        limiter.acquire(len(remaining))
        for recipient in remaining:
            attempts[recipient.email] += 1
        try:
            response = client.send_bulk_email(**build_bulk_request(remaining, settings))
        except ClientError as error:
            code = _client_error_code(error)
            detail = sanitize_detail(
                error.response.get("Error", {}).get("Message", code)
            )
            if code in RETRYABLE_CLIENT_CODES:
                limiter.record_throttle()
                retryable = [
                    recipient
                    for recipient in remaining
                    if attempts[recipient.email] < MAX_APPLICATION_ATTEMPTS
                ]
                outcomes.extend(
                    EntryOutcome(
                        recipient.email,
                        RecipientStatus.RETRY_EXHAUSTED,
                        attempts[recipient.email],
                        reason_code=code,
                        detail=detail,
                    )
                    for recipient in remaining
                    if attempts[recipient.email] >= MAX_APPLICATION_ATTEMPTS
                )
                if retryable:
                    sleeper(
                        full_jitter_delay(
                            max(attempts[item.email] for item in retryable),
                            random_source,
                        )
                    )
                remaining = retryable
                continue
            if code in GLOBAL_STOP_CLIENT_CODES:
                outcomes.extend(
                    EntryOutcome(
                        recipient.email,
                        RecipientStatus.RETRYABLE,
                        attempts[recipient.email],
                        reason_code=code,
                        detail=detail,
                    )
                    for recipient in remaining
                )
                return BatchOutcome(tuple(outcomes), global_stop=code)
            outcomes.extend(
                EntryOutcome(
                    recipient.email,
                    RecipientStatus.PERMANENT_FAILURE,
                    attempts[recipient.email],
                    reason_code=code,
                    detail=detail,
                )
                for recipient in remaining
            )
            return BatchOutcome(tuple(outcomes))
        except BotoCoreError as error:
            return BatchOutcome(
                _unknown_outcomes(
                    remaining,
                    attempts,
                    type(error).__name__,
                    str(error),
                )
                + tuple(outcomes)
            )
        results = response.get("BulkEmailEntryResults", [])
        if len(results) != len(remaining):
            return BatchOutcome(
                tuple(outcomes)
                + _unknown_outcomes(
                    remaining,
                    attempts,
                    "response_length_mismatch",
                    "SES result count did not match the submitted batch",
                )
            )

        retry_next: list[Recipient] = []
        global_stop: str | None = None
        batch_was_successful = True
        for recipient, result in zip(remaining, results, strict=True):
            status = str(result.get("Status", "FAILED"))
            message_id = result.get("MessageId")
            error_text = sanitize_detail(result.get("Error", status), recipient.email)
            current_attempts = attempts[recipient.email]
            if status == "SUCCESS":
                outcomes.append(
                    EntryOutcome(
                        recipient.email,
                        RecipientStatus.ACCEPTED,
                        current_attempts,
                        message_id=str(message_id) if message_id else None,
                    )
                )
                continue
            batch_was_successful = False
            if status in RETRYABLE_ENTRY_STATUSES:
                if status == "ACCOUNT_THROTTLED":
                    limiter.record_throttle()
                if current_attempts < MAX_APPLICATION_ATTEMPTS:
                    retry_next.append(recipient)
                else:
                    outcomes.append(
                        EntryOutcome(
                            recipient.email,
                            RecipientStatus.RETRY_EXHAUSTED,
                            current_attempts,
                            reason_code=status.lower(),
                            detail=error_text,
                        )
                    )
                continue
            if status in GLOBAL_STOP_ENTRY_STATUSES:
                outcomes.append(
                    EntryOutcome(
                        recipient.email,
                        RecipientStatus.RETRYABLE,
                        current_attempts,
                        reason_code=status.lower(),
                        detail=error_text,
                    )
                )
                global_stop = status.lower()
                continue
            outcomes.append(
                EntryOutcome(
                    recipient.email,
                    RecipientStatus.PERMANENT_FAILURE,
                    current_attempts,
                    reason_code=status.lower(),
                    detail=error_text,
                )
            )

        if global_stop:
            outcomes.extend(
                EntryOutcome(
                    recipient.email,
                    RecipientStatus.RETRYABLE,
                    attempts[recipient.email],
                    reason_code=global_stop,
                    detail="Batch stopped by an account-level SES result",
                )
                for recipient in retry_next
            )
            return BatchOutcome(tuple(outcomes), global_stop=global_stop)
        if batch_was_successful:
            limiter.record_success()
        if retry_next:
            sleeper(
                full_jitter_delay(
                    max(attempts[item.email] for item in retry_next),
                    random_source,
                )
            )
        remaining = retry_next

    return BatchOutcome(tuple(outcomes))


def count_work(connection: sqlite3.Connection, campaign: str) -> int:
    return int(
        connection.execute(
            """
            SELECT COUNT(*) FROM recipient
            WHERE campaign_id = ? AND status IN ('pending', 'retryable')
            """,
            (campaign,),
        ).fetchone()[0]
    )


def fetch_work_batch(
    connection: sqlite3.Connection,
    campaign: str,
    limit: int,
) -> list[Recipient]:
    rows = connection.execute(
        """
        SELECT email, name, attempts
        FROM recipient
        WHERE campaign_id = ? AND status IN ('pending', 'retryable')
        ORDER BY email
        LIMIT ?
        """,
        (campaign, limit),
    ).fetchall()
    recipients = [
        Recipient(row["email"], row["name"], int(row["attempts"])) for row in rows
    ]
    if recipients:
        now = utc_now()
        with connection:
            connection.executemany(
                """
                UPDATE recipient SET status = 'in_flight', updated_at = ?
                WHERE campaign_id = ? AND email = ?
                """,
                [(now, campaign, recipient.email) for recipient in recipients],
            )
    return recipients


def persist_batch_outcome(
    connection: sqlite3.Connection,
    campaign: str,
    outcome: BatchOutcome,
) -> None:
    now = utc_now()
    with connection:
        connection.executemany(
            """
            UPDATE recipient
            SET status = ?, attempts = ?, message_id = ?,
                last_reason_code = ?, last_detail = ?, updated_at = ?
            WHERE campaign_id = ? AND email = ?
            """,
            [
                (
                    entry.status.value,
                    entry.attempts,
                    entry.message_id,
                    entry.reason_code,
                    entry.detail,
                    now,
                    campaign,
                    entry.email,
                )
                for entry in outcome.entries
            ],
        )


def aggregate_counts(connection: sqlite3.Connection, campaign: str) -> dict[str, int]:
    counts = {status.value: 0 for status in RecipientStatus}
    for row in connection.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM recipient WHERE campaign_id = ? GROUP BY status
        """,
        (campaign,),
    ):
        counts[str(row["status"])] = int(row["count"])
    counts["input_bad_row"] = int(
        connection.execute(
            """
            SELECT COUNT(*) FROM input_issue
            WHERE campaign_id = ? AND status = 'bad_row'
            """,
            (campaign,),
        ).fetchone()[0]
    )
    counts["input_duplicate"] = int(
        connection.execute(
            """
            SELECT COUNT(*) FROM input_issue
            WHERE campaign_id = ? AND status = 'duplicate'
            """,
            (campaign,),
        ).fetchone()[0]
    )
    counts["total"] = (
        sum(
            value
            for key, value in counts.items()
            if key not in {"input_bad_row", "input_duplicate", "total"}
        )
        + counts["input_bad_row"]
        + counts["input_duplicate"]
    )
    return counts


def run_scheduler(
    connection: sqlite3.Connection,
    client: Any,
    settings: Settings,
    send_budget: int,
    target_rate: float,
    *,
    sleeper: Callable[[float], None] = time.sleep,
) -> SchedulerResult:
    limiter = RecipientRateLimiter(target_rate, sleeper=sleeper)
    stop_requested = threading.Event()
    interrupted = False
    previous_handler: Any = None
    if threading.current_thread() is threading.main_thread():
        previous_handler = signal.getsignal(signal.SIGINT)

        def handle_sigint(_signum: int, _frame: Any) -> None:
            stop_requested.set()

        signal.signal(signal.SIGINT, handle_sigint)

    outstanding: dict[Future[BatchOutcome], int] = {}
    scheduled = 0
    max_outstanding = 0
    stop_reason: str | None = None
    started = time.monotonic()
    last_progress = started
    pool = ThreadPoolExecutor(max_workers=settings.workers)
    try:
        while outstanding or (not stop_requested.is_set() and scheduled < send_budget):
            while (
                not stop_requested.is_set()
                and stop_reason is None
                and scheduled < send_budget
                and len(outstanding) < 2 * settings.workers
            ):
                batch = fetch_work_batch(
                    connection,
                    settings.campaign_id,
                    min(BATCH_SIZE, send_budget - scheduled),
                )
                if not batch:
                    break
                future = pool.submit(
                    send_batch,
                    client,
                    settings,
                    batch,
                    limiter,
                    sleeper=sleeper,
                )
                outstanding[future] = len(batch)
                max_outstanding = max(max_outstanding, len(outstanding))
                scheduled += len(batch)

            if not outstanding:
                break
            completed, _ = wait(
                outstanding,
                timeout=1.0,
                return_when=FIRST_COMPLETED,
            )
            for future in completed:
                outstanding.pop(future)
                outcome = future.result()
                persist_batch_outcome(connection, settings.campaign_id, outcome)
                if outcome.global_stop and stop_reason is None:
                    stop_reason = outcome.global_stop
                    stop_requested.set()

            now = time.monotonic()
            if now - last_progress >= 5:
                counts = aggregate_counts(connection, settings.campaign_id)
                elapsed = max(now - started, 0.001)
                LOGGER.info(
                    "progress terminal=%d/%d accepted=%d bad_rows=%d "
                    "failures=%d unknown=%d rate=%.1f/s target=%.1f/s "
                    "budget_remaining=%d elapsed=%.1fs",
                    sum(counts.get(status, 0) for status in COMPLETED_STATUSES)
                    + counts["input_bad_row"]
                    + counts["input_duplicate"],
                    counts["total"],
                    counts["accepted"],
                    counts["bad_row"] + counts["input_bad_row"],
                    counts["permanent_failure"] + counts["retry_exhausted"],
                    counts["unknown"],
                    counts["accepted"] / elapsed,
                    limiter.current_rate,
                    max(0, send_budget - scheduled),
                    elapsed,
                )
                last_progress = now

        if stop_requested.is_set() and stop_reason is None:
            interrupted = True
            stop_reason = "interrupted"
        if (
            stop_reason is None
            and scheduled >= send_budget
            and count_work(connection, settings.campaign_id) > 0
        ):
            stop_reason = "rolling_quota_exhausted"
    finally:
        pool.shutdown(wait=True)
        if previous_handler is not None:
            signal.signal(signal.SIGINT, previous_handler)

    return SchedulerResult(stop_reason, scheduled, interrupted, max_outstanding)


def _artifact_paths(settings: Settings) -> tuple[Path, Path]:
    summary = settings.state_path.with_name(
        f"campaign-summary-{settings.campaign_id}.json"
    )
    failures = settings.state_path.with_name(
        f"failure-report-{settings.campaign_id}.csv"
    )
    return summary, failures


def _atomic_text_writer(path: Path) -> tuple[Any, Path]:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        text=True,
    )
    temporary_path = Path(temporary_name)
    os.chmod(temporary_path, 0o600)
    temporary = os.fdopen(
        descriptor,
        mode="w",
        encoding="utf-8",
        newline="",
    )
    return temporary, temporary_path


def write_reports(
    connection: sqlite3.Connection,
    settings: Settings,
    *,
    stop_reason: str | None = None,
) -> tuple[Path, Path]:
    summary_path, failure_path = _artifact_paths(settings)
    counts = aggregate_counts(connection, settings.campaign_id)
    summary = {
        "campaign_id": settings.campaign_id,
        "counts": counts,
        "stop_reason": stop_reason,
        "complete": not any(counts.get(status, 0) for status in INCOMPLETE_STATUSES),
        "generated_at": utc_now(),
    }

    summary_file, summary_temp = _atomic_text_writer(summary_path)
    try:
        json.dump(summary, summary_file, indent=2, sort_keys=True)
        summary_file.write("\n")
        summary_file.flush()
        os.fsync(summary_file.fileno())
        summary_file.close()
        os.replace(summary_temp, summary_path)
        os.chmod(summary_path, 0o600)
    except Exception:
        summary_file.close()
        summary_temp.unlink(missing_ok=True)
        raise

    failure_file, failure_temp = _atomic_text_writer(failure_path)
    try:
        writer = csv.DictWriter(
            failure_file,
            fieldnames=[
                "email",
                "stage",
                "reason_code",
                "disposition",
                "detail",
            ],
        )
        writer.writeheader()
        for row in connection.execute(
            """
            SELECT COALESCE(raw_email, '') AS email, 'input' AS stage,
                   reason_code, status AS disposition, COALESCE(detail, '') AS detail
            FROM input_issue
            WHERE campaign_id = ? AND status = 'bad_row'
            ORDER BY row_number
            """,
            (settings.campaign_id,),
        ):
            writer.writerow(dict(row))
        for row in connection.execute(
            """
            SELECT email, 'submission' AS stage,
                   COALESCE(last_reason_code, status) AS reason_code,
                   status AS disposition, COALESCE(last_detail, '') AS detail
            FROM recipient
            WHERE campaign_id = ?
              AND status IN (
                  'bad_row', 'permanent_failure', 'retryable',
                  'retry_exhausted', 'unknown'
              )
            ORDER BY email
            """,
            (settings.campaign_id,),
        ):
            writer.writerow(dict(row))
        failure_file.flush()
        os.fsync(failure_file.fileno())
        failure_file.close()
        os.replace(failure_temp, failure_path)
        os.chmod(failure_path, 0o600)
    except Exception:
        failure_file.close()
        failure_temp.unlink(missing_ok=True)
        raise

    return summary_path, failure_path


def campaign_is_complete(counts: dict[str, int]) -> bool:
    return not any(counts.get(status, 0) for status in INCOMPLETE_STATUSES)


def execute(
    settings: Settings,
    *,
    client_factory: Callable[[Settings], Any] = create_ses_client,
) -> int:
    connection = connect_state(settings.state_path)
    effective = settings
    campaign_ready = False
    stop_reason: str | None = None
    try:
        effective, import_stats = resolve_campaign(connection, settings)
        campaign_ready = True
        stale = reset_stale_in_flight(connection, effective.campaign_id)
        if stale:
            LOGGER.warning("Marked %d stale in-flight recipients unknown", stale)
        apply_recovery_actions(connection, effective)
        if import_stats is not None:
            LOGGER.info(
                "imported rows=%d unique=%d bad_rows=%d duplicates=%d",
                import_stats.rows,
                import_stats.unique_recipients,
                import_stats.bad_rows,
                import_stats.duplicate_rows,
            )

        client = client_factory(effective)
        suppressed = mark_suppressed_destinations(
            connection, client, effective.campaign_id
        )
        pending = count_work(connection, effective.campaign_id)
        result = preflight(client, pending, effective)
        estimated_seconds = (
            pending / result.target_rate if result.target_rate > 0 else math.inf
        )
        LOGGER.info(
            "preflight recipients=%d suppressed=%d budget=%d target_rate=%.1f/s "
            "requests=%d estimated_minimum=%.1fs",
            pending,
            suppressed,
            result.send_budget,
            result.target_rate,
            sum(1 for _size in planned_batch_sizes(pending)),
            estimated_seconds,
        )

        if not effective.send:
            LOGGER.info("dry run complete; SendBulkEmail was not called")
            return 0
        if pending and result.send_budget == 0:
            stop_reason = "rolling_quota_exhausted"
            raise CampaignError(
                "rolling 24-hour SES quota has no current capacity; "
                "progress is checkpointed, rerun after quota replenishes"
            )

        scheduler = run_scheduler(
            connection,
            client,
            effective,
            result.send_budget,
            result.target_rate,
        )
        stop_reason = scheduler.stop_reason
        if stop_reason:
            LOGGER.error(
                "campaign stopped reason=%s scheduled=%d; progress is checkpointed",
                stop_reason,
                scheduler.scheduled_recipients,
            )
        counts = aggregate_counts(connection, effective.campaign_id)
        return 0 if campaign_is_complete(counts) and stop_reason is None else 2
    except CampaignError as error:
        LOGGER.error("%s", sanitize_detail(error))
        return 2
    except (BotoCoreError, ClientError) as error:
        LOGGER.error("AWS preflight failed: %s", sanitize_detail(error))
        return 2
    finally:
        if campaign_ready:
            summary, failures = write_reports(
                connection,
                effective,
                stop_reason=stop_reason,
            )
            LOGGER.info(
                "wrote aggregate summary and protected failure report: %s, %s",
                summary.name,
                failures.name,
            )
        connection.close()


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    try:
        settings = parse_settings(argv)
    except CampaignError as error:
        LOGGER.error("%s", error)
        return 2
    return execute(settings)


if __name__ == "__main__":
    raise SystemExit(main())
