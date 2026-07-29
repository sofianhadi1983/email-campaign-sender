from __future__ import annotations

import csv
import json
import logging
import os
import sqlite3
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any

import boto3
import pytest
from botocore.exceptions import ClientError, ReadTimeoutError
from botocore.stub import Stubber

import campaign_sender as sender


class FakeSES:
    def __init__(
        self,
        send_responses: list[dict[str, Any] | Exception] | None = None,
        *,
        suppressed: list[str] | None = None,
        max_24_hour_send: float = 1_000_000,
        sent_last_24_hours: float = 0,
        max_send_rate: float = 1_000,
    ) -> None:
        self.send_responses = deque(send_responses or [])
        self.send_calls: list[dict[str, Any]] = []
        self.suppressed = suppressed or []
        self.max_24_hour_send = max_24_hour_send
        self.sent_last_24_hours = sent_last_24_hours
        self.max_send_rate = max_send_rate

    def list_suppressed_destinations(self, **_kwargs: Any) -> dict[str, Any]:
        return {
            "SuppressedDestinationSummaries": [
                {"EmailAddress": email} for email in self.suppressed
            ]
        }

    def get_account(self) -> dict[str, Any]:
        return {
            "ProductionAccessEnabled": True,
            "SendingEnabled": True,
            "EnforcementStatus": "HEALTHY",
            "SendQuota": {
                "Max24HourSend": self.max_24_hour_send,
                "SentLast24Hours": self.sent_last_24_hours,
                "MaxSendRate": self.max_send_rate,
            },
            "SuppressionAttributes": {"SuppressedReasons": ["BOUNCE", "COMPLAINT"]},
        }

    def get_email_identity(self, **_kwargs: Any) -> dict[str, Any]:
        return {"VerifiedForSendingStatus": True}

    def get_email_template(self, **_kwargs: Any) -> dict[str, Any]:
        return {
            "TemplateContent": {
                "Subject": "Hello {{name}}",
                "Text": "Promotion for {{name}}",
                "Html": "<p>Promotion for {{name}}</p>",
            }
        }

    def test_render_email_template(self, **_kwargs: Any) -> dict[str, Any]:
        return {"RenderedTemplate": "ok"}

    def get_configuration_set(self, **_kwargs: Any) -> dict[str, Any]:
        return {}

    def get_configuration_set_event_destinations(
        self, **_kwargs: Any
    ) -> dict[str, Any]:
        return {
            "EventDestinations": [
                {
                    "Enabled": True,
                    "MatchingEventTypes": sorted(sender.REQUIRED_EVENTS),
                }
            ]
        }

    def send_bulk_email(self, **kwargs: Any) -> dict[str, Any]:
        self.send_calls.append(kwargs)
        if self.send_responses:
            response = self.send_responses.popleft()
            if isinstance(response, Exception):
                raise response
            return response
        return success_response(len(kwargs["BulkEmailEntries"]))


def success_response(count: int) -> dict[str, Any]:
    return {
        "BulkEmailEntryResults": [
            {"Status": "SUCCESS", "MessageId": f"message-{index}"}
            for index in range(count)
        ]
    }


def entry_response(*statuses: str) -> dict[str, Any]:
    return {
        "BulkEmailEntryResults": [
            {
                "Status": status,
                "MessageId": f"message-{index}" if status == "SUCCESS" else None,
                "Error": f"{status} diagnostic",
            }
            for index, status in enumerate(statuses)
        ]
    }


@pytest.fixture
def fixture_csv() -> Path:
    return Path(__file__).parent / "fixtures" / "recipients.csv"


def make_settings(
    tmp_path: Path,
    input_path: Path,
    **changes: Any,
) -> sender.Settings:
    settings = sender.Settings(
        input_path=input_path.resolve(),
        campaign_id="test-campaign",
        region="us-east-1",
        from_email="sender@example.com",
        template_name="promotion",
        configuration_set="events",
        state_path=tmp_path / "state.sqlite3",
        workers=2,
    )
    return sender.replace(settings, **changes)


def imported_state(
    tmp_path: Path,
    fixture_csv: Path,
) -> tuple[sqlite3.Connection, sender.Settings, sender.ImportStats]:
    settings = make_settings(tmp_path, fixture_csv)
    connection = sender.connect_state(settings.state_path)
    stats = sender.import_recipients(fixture_csv, connection, settings)
    return connection, settings, stats


def test_import_streams_validates_deduplicates_and_marks_conflicts(
    tmp_path: Path,
    fixture_csv: Path,
) -> None:
    connection, settings, stats = imported_state(tmp_path, fixture_csv)
    try:
        assert stats.rows == 8
        assert stats.valid_rows == 5
        assert stats.unique_recipients == 3
        assert stats.bad_rows == 5
        assert stats.duplicate_rows == 1
        rows = connection.execute(
            "SELECT email, status FROM recipient WHERE campaign_id = ? ORDER BY email",
            (settings.campaign_id,),
        ).fetchall()
        assert [(row["email"], row["status"]) for row in rows] == [
            ("ada@example.com", "pending"),
            ("grace@example.com", "bad_row"),
            ("user@xn--bcher-kva.example", "pending"),
        ]
        reasons = {
            row["reason_code"]
            for row in connection.execute(
                """
                SELECT reason_code FROM input_issue
                WHERE campaign_id = ? AND status = 'bad_row'
                """,
                (settings.campaign_id,),
            )
        }
        assert reasons == {
            "duplicate_conflict",
            "invalid_email",
            "missing_email",
            "missing_name",
        }
        counts = sender.aggregate_counts(connection, settings.campaign_id)
        assert counts["input_duplicate"] == 1
        assert counts["total"] == stats.rows
        assert (os.stat(settings.state_path).st_mode & 0o777) == 0o600
    finally:
        connection.close()


def test_import_rejects_wrong_headers(tmp_path: Path) -> None:
    csv_path = tmp_path / "wrong.csv"
    csv_path.write_text("email,first_name\nperson@example.com,Person\n")
    settings = make_settings(tmp_path, csv_path)
    connection = sender.connect_state(settings.state_path)
    try:
        with pytest.raises(sender.CampaignError, match="exactly name,email"):
            sender.import_recipients(csv_path, connection, settings)
        assert connection.execute("SELECT COUNT(*) FROM campaign").fetchone()[0] == 0
    finally:
        connection.close()


def test_resume_uses_first_campaign_as_source_of_truth(
    tmp_path: Path,
    fixture_csv: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    connection, settings, _stats = imported_state(tmp_path, fixture_csv)
    changed = sender.replace(
        settings,
        input_path=tmp_path / "changed.csv",
        region="eu-west-1",
        from_email="changed@example.net",
        template_name="changed",
        configuration_set="changed",
        workers=5,
    )
    try:
        with caplog.at_level(logging.WARNING):
            effective, stats = sender.resolve_campaign(connection, changed)
        assert stats is None
        assert effective.input_path == settings.input_path
        assert effective.region == settings.region
        assert effective.from_email == settings.from_email
        assert effective.template_name == settings.template_name
        assert effective.configuration_set == settings.configuration_set
        assert effective.workers == 5
        assert "input" in caplog.text
        assert "changed@example.net" not in caplog.text
    finally:
        connection.close()


def test_recovery_actions_are_explicit(
    tmp_path: Path,
    fixture_csv: Path,
) -> None:
    connection, settings, _stats = imported_state(tmp_path, fixture_csv)
    try:
        connection.execute(
            """
            UPDATE recipient SET status = 'retry_exhausted', attempts = 3
            WHERE campaign_id = ? AND email = 'ada@example.com'
            """,
            (settings.campaign_id,),
        )
        connection.execute(
            """
            UPDATE recipient SET status = 'unknown', attempts = 1
            WHERE campaign_id = ? AND email LIKE 'user@%'
            """,
            (settings.campaign_id,),
        )
        connection.commit()
        sender.apply_recovery_actions(connection, settings)
        assert sender.count_work(connection, settings.campaign_id) == 0

        sender.apply_recovery_actions(
            connection,
            sender.replace(
                settings,
                send=True,
                confirm_opted_in=True,
                retry_exhausted=True,
                retry_unknown=True,
                accept_duplicate_risk=True,
            ),
        )
        assert sender.count_work(connection, settings.campaign_id) == 2
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"send": True}, "--send requires"),
        (
            {"retry_exhausted": True},
            "recovery flags require",
        ),
        (
            {"send": True, "confirm_opted_in": True, "retry_unknown": True},
            "requires --accept-duplicate-risk",
        ),
    ],
)
def test_live_and_recovery_flags_are_guarded(
    tmp_path: Path,
    fixture_csv: Path,
    changes: dict[str, Any],
    message: str,
) -> None:
    settings = make_settings(tmp_path, fixture_csv, **changes)
    with pytest.raises(sender.CampaignError, match=message):
        sender.validate_safety_flags(settings)


def test_build_bulk_request_has_one_recipient_and_escaped_json(
    tmp_path: Path,
    fixture_csv: Path,
) -> None:
    settings = make_settings(tmp_path, fixture_csv)
    recipients = [
        sender.Recipient(f"user-{index}@example.com", f'Name "{index}" / ☃')
        for index in range(50)
    ]
    request = sender.build_bulk_request(recipients, settings)
    entries = request["BulkEmailEntries"]
    assert len(entries) == 50
    for index, entry in enumerate(entries):
        assert entry["Destination"]["ToAddresses"] == [f"user-{index}@example.com"]
        data = entry["ReplacementEmailContent"]["ReplacementTemplate"][
            "ReplacementTemplateData"
        ]
        assert json.loads(data) == {"name": f'Name "{index}" / ☃'}
    with pytest.raises(ValueError):
        sender.build_bulk_request([], settings)


def test_bulk_request_matches_the_real_botocore_model(
    tmp_path: Path,
    fixture_csv: Path,
) -> None:
    settings = make_settings(tmp_path, fixture_csv)
    request = sender.build_bulk_request(
        [sender.Recipient("person@example.com", "Person")],
        settings,
    )
    client = boto3.client(
        "sesv2",
        region_name=settings.region,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    with Stubber(client) as stubber:
        stubber.add_response("send_bulk_email", success_response(1), request)
        assert (
            client.send_bulk_email(**request)["BulkEmailEntryResults"][0]["Status"]
            == "SUCCESS"
        )


def test_one_million_recipients_require_twenty_thousand_batches() -> None:
    batches = sender.planned_batch_sizes(1_000_000)
    count = 0
    total = 0
    for size in batches:
        count += 1
        total += size
        assert size == 50
    assert count == 20_000
    assert total == 1_000_000


class FakeClock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.value += seconds


def test_rate_limiter_accounts_for_recipient_tokens_and_adapts() -> None:
    clock = FakeClock()
    limiter = sender.RecipientRateLimiter(10, clock=clock, sleeper=clock.sleep)
    limiter.acquire(50)
    limiter.acquire(50)
    assert clock.value == pytest.approx(5)
    limiter.record_throttle()
    assert limiter.current_rate == pytest.approx(8)
    for _ in range(10):
        limiter.record_success()
    assert limiter.current_rate == pytest.approx(8.4)


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("SUCCESS", sender.RecipientStatus.ACCEPTED),
        ("INVALID_PARAMETER", sender.RecipientStatus.PERMANENT_FAILURE),
        ("MESSAGE_REJECTED", sender.RecipientStatus.PERMANENT_FAILURE),
        ("MAIL_FROM_DOMAIN_NOT_VERIFIED", sender.RecipientStatus.RETRYABLE),
        ("CONFIGURATION_SET_NOT_FOUND", sender.RecipientStatus.RETRYABLE),
        ("TEMPLATE_NOT_FOUND", sender.RecipientStatus.RETRYABLE),
        ("ACCOUNT_SUSPENDED", sender.RecipientStatus.RETRYABLE),
        ("ACCOUNT_SENDING_PAUSED", sender.RecipientStatus.RETRYABLE),
        ("CONFIGURATION_SET_SENDING_PAUSED", sender.RecipientStatus.RETRYABLE),
        ("ACCOUNT_DAILY_QUOTA_EXCEEDED", sender.RecipientStatus.RETRYABLE),
        ("INVALID_SENDING_POOL_NAME", sender.RecipientStatus.PERMANENT_FAILURE),
    ],
)
def test_entry_status_classification(
    tmp_path: Path,
    fixture_csv: Path,
    status: str,
    expected: sender.RecipientStatus,
) -> None:
    settings = make_settings(tmp_path, fixture_csv)
    client = FakeSES([entry_response(status)])
    outcome = sender.send_batch(
        client,
        settings,
        [sender.Recipient("person@example.com", "Person")],
        sender.RecipientRateLimiter(1000),
        sleeper=lambda _seconds: None,
    )
    assert outcome.entries[0].status == expected


@pytest.mark.parametrize(
    "status",
    ["ACCOUNT_THROTTLED", "TRANSIENT_FAILURE", "FAILED"],
)
def test_every_retryable_entry_status_exhausts_at_three(
    tmp_path: Path,
    fixture_csv: Path,
    status: str,
) -> None:
    settings = make_settings(tmp_path, fixture_csv)
    client = FakeSES([entry_response(status)])
    outcome = sender.send_batch(
        client,
        settings,
        [sender.Recipient("person@example.com", "Person", attempts=2)],
        sender.RecipientRateLimiter(1000),
        sleeper=lambda _seconds: None,
    )
    assert len(client.send_calls) == 1
    assert outcome.entries[0].status == sender.RecipientStatus.RETRY_EXHAUSTED
    assert outcome.entries[0].attempts == 3


def test_retryable_result_stops_after_three_total_attempts(
    tmp_path: Path,
    fixture_csv: Path,
) -> None:
    settings = make_settings(tmp_path, fixture_csv)
    client = FakeSES(
        [
            entry_response("TRANSIENT_FAILURE"),
            entry_response("FAILED"),
            entry_response("ACCOUNT_THROTTLED"),
        ]
    )
    delays: list[float] = []
    outcome = sender.send_batch(
        client,
        settings,
        [sender.Recipient("person@example.com", "Person")],
        sender.RecipientRateLimiter(1000),
        rng=sender.random.Random(1),
        sleeper=delays.append,
    )
    assert len(client.send_calls) == 3
    assert len(delays) == 2
    assert outcome.entries == (
        sender.EntryOutcome(
            "person@example.com",
            sender.RecipientStatus.RETRY_EXHAUSTED,
            3,
            reason_code="account_throttled",
            detail="ACCOUNT_THROTTLED diagnostic",
        ),
    )


def test_ambiguous_timeout_is_unknown_without_retry(
    tmp_path: Path,
    fixture_csv: Path,
) -> None:
    settings = make_settings(tmp_path, fixture_csv)
    timeout = ReadTimeoutError(endpoint_url="https://email.example")
    client = FakeSES([timeout])
    outcome = sender.send_batch(
        client,
        settings,
        [sender.Recipient("person@example.com", "Person")],
        sender.RecipientRateLimiter(1000),
        sleeper=lambda _seconds: None,
    )
    assert len(client.send_calls) == 1
    assert outcome.entries[0].status == sender.RecipientStatus.UNKNOWN
    assert outcome.entries[0].reason_code == "ReadTimeoutError"


def test_response_length_mismatch_is_unknown(
    tmp_path: Path,
    fixture_csv: Path,
) -> None:
    settings = make_settings(tmp_path, fixture_csv)
    client = FakeSES([{"BulkEmailEntryResults": []}])
    outcome = sender.send_batch(
        client,
        settings,
        [sender.Recipient("person@example.com", "Person")],
        sender.RecipientRateLimiter(1000),
        sleeper=lambda _seconds: None,
    )
    assert outcome.entries[0].status == sender.RecipientStatus.UNKNOWN
    assert outcome.entries[0].reason_code == "response_length_mismatch"


def test_explicit_throttling_retries_then_succeeds(
    tmp_path: Path,
    fixture_csv: Path,
) -> None:
    settings = make_settings(tmp_path, fixture_csv)
    throttle = ClientError(
        {"Error": {"Code": "ThrottlingException", "Message": "slow down"}},
        "SendBulkEmail",
    )
    client = FakeSES([throttle, success_response(1)])
    limiter = sender.RecipientRateLimiter(1000)
    outcome = sender.send_batch(
        client,
        settings,
        [sender.Recipient("person@example.com", "Person")],
        limiter,
        rng=sender.random.Random(1),
        sleeper=lambda _seconds: None,
    )
    assert len(client.send_calls) == 2
    assert outcome.entries[0].status == sender.RecipientStatus.ACCEPTED
    assert outcome.entries[0].attempts == 2
    assert limiter.current_rate == pytest.approx(800)


def _insert_pending(
    connection: sqlite3.Connection,
    campaign: str,
    count: int,
) -> None:
    with connection:
        connection.executemany(
            """
            INSERT INTO recipient (
                campaign_id, email, name, status, attempts, updated_at
            ) VALUES (?, ?, ?, 'pending', 0, ?)
            """,
            [
                (
                    campaign,
                    f"user-{index:04d}@example.com",
                    f"User {index}",
                    sender.utc_now(),
                )
                for index in range(count)
            ],
        )


def test_scheduler_honors_daily_budget_and_checkpoints(
    tmp_path: Path,
    fixture_csv: Path,
) -> None:
    settings = make_settings(tmp_path, fixture_csv, workers=2)
    connection = sender.connect_state(settings.state_path)
    try:
        connection.execute(
            """
            INSERT INTO campaign VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                settings.campaign_id,
                str(settings.input_path),
                "digest",
                settings.region,
                settings.from_email,
                settings.template_name,
                settings.configuration_set,
                sender.utc_now(),
            ),
        )
        _insert_pending(connection, settings.campaign_id, 120)
        result = sender.run_scheduler(
            connection,
            FakeSES(),
            settings,
            send_budget=50,
            target_rate=1000,
            sleeper=lambda _seconds: None,
        )
        counts = sender.aggregate_counts(connection, settings.campaign_id)
        assert result.scheduled_recipients == 50
        assert result.stop_reason == "rolling_quota_exhausted"
        assert result.max_outstanding <= 2 * settings.workers
        assert counts["accepted"] == 50
        assert counts["pending"] == 70
    finally:
        connection.close()


def test_suppression_is_streamed_into_state(
    tmp_path: Path,
    fixture_csv: Path,
) -> None:
    connection, settings, _stats = imported_state(tmp_path, fixture_csv)
    try:
        changed = sender.mark_suppressed_destinations(
            connection, FakeSES(suppressed=["ADA@EXAMPLE.COM"]), settings.campaign_id
        )
        assert changed == 1
        row = connection.execute(
            "SELECT status FROM recipient WHERE email = 'ada@example.com'"
        ).fetchone()
        assert row["status"] == "suppressed"
    finally:
        connection.close()


def test_suppression_follows_explicit_next_tokens(
    tmp_path: Path,
    fixture_csv: Path,
) -> None:
    class PagedSuppressionSES:
        def __init__(self) -> None:
            self.requests: list[dict[str, Any]] = []

        def list_suppressed_destinations(self, **kwargs: Any) -> dict[str, Any]:
            self.requests.append(kwargs)
            if not kwargs:
                return {
                    "SuppressedDestinationSummaries": [
                        {"EmailAddress": "ada@example.com"}
                    ],
                    "NextToken": "page-2",
                }
            assert kwargs == {"NextToken": "page-2"}
            return {
                "SuppressedDestinationSummaries": [
                    {"EmailAddress": "user@xn--bcher-kva.example"}
                ]
            }

    connection, settings, _stats = imported_state(tmp_path, fixture_csv)
    client = PagedSuppressionSES()
    try:
        changed = sender.mark_suppressed_destinations(
            connection, client, settings.campaign_id
        )
        assert changed == 2
        assert client.requests == [{}, {"NextToken": "page-2"}]
    finally:
        connection.close()


def test_preflight_checks_resources_and_computes_partial_budget(
    tmp_path: Path,
    fixture_csv: Path,
) -> None:
    settings = make_settings(tmp_path, fixture_csv)
    result = sender.preflight(
        FakeSES(
            max_24_hour_send=1_000,
            sent_last_24_hours=925.4,
            max_send_rate=100,
        ),
        pending_count=200,
        settings=settings,
    )
    assert result.send_budget == 74
    assert result.target_rate == 90


def test_preflight_rejects_missing_event_type(
    tmp_path: Path,
    fixture_csv: Path,
) -> None:
    class MissingEventsSES(FakeSES):
        def get_configuration_set_event_destinations(
            self, **_kwargs: Any
        ) -> dict[str, Any]:
            return {
                "EventDestinations": [
                    {"Enabled": True, "MatchingEventTypes": ["DELIVERY"]}
                ]
            }

    with pytest.raises(sender.PreflightError, match="missing"):
        sender.preflight(
            MissingEventsSES(),
            pending_count=1,
            settings=make_settings(tmp_path, fixture_csv),
        )


def test_mixed_http_success_results_are_persisted_per_recipient(
    tmp_path: Path,
    fixture_csv: Path,
) -> None:
    settings = make_settings(tmp_path, fixture_csv)
    connection = sender.connect_state(settings.state_path)
    try:
        connection.execute(
            "INSERT INTO campaign VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                settings.campaign_id,
                str(settings.input_path),
                "digest",
                settings.region,
                settings.from_email,
                settings.template_name,
                settings.configuration_set,
                sender.utc_now(),
            ),
        )
        _insert_pending(connection, settings.campaign_id, 2)
        recipients = sender.fetch_work_batch(connection, settings.campaign_id, 2)
        outcome = sender.send_batch(
            FakeSES([entry_response("SUCCESS", "MESSAGE_REJECTED")]),
            settings,
            recipients,
            sender.RecipientRateLimiter(1000),
            sleeper=lambda _seconds: None,
        )
        sender.persist_batch_outcome(connection, settings.campaign_id, outcome)
        statuses = [
            row["status"]
            for row in connection.execute("SELECT status FROM recipient ORDER BY email")
        ]
        assert statuses == ["accepted", "permanent_failure"]
    finally:
        connection.close()


def test_stale_in_flight_becomes_verbose_unknown(
    tmp_path: Path,
    fixture_csv: Path,
) -> None:
    connection, settings, _stats = imported_state(tmp_path, fixture_csv)
    try:
        connection.execute(
            """
            UPDATE recipient SET status = 'in_flight'
            WHERE campaign_id = ? AND email = 'ada@example.com'
            """,
            (settings.campaign_id,),
        )
        connection.commit()
        assert sender.reset_stale_in_flight(connection, settings.campaign_id) == 1
        row = connection.execute(
            """
            SELECT status, last_reason_code FROM recipient
            WHERE email = 'ada@example.com'
            """
        ).fetchone()
        assert (row["status"], row["last_reason_code"]) == (
            "unknown",
            "interrupted_in_flight",
        )
    finally:
        connection.close()


def test_reports_are_protected_verbose_and_aggregates_are_redacted(
    tmp_path: Path,
    fixture_csv: Path,
) -> None:
    connection, settings, _stats = imported_state(tmp_path, fixture_csv)
    try:
        connection.execute(
            """
            UPDATE recipient
            SET status = 'unknown', attempts = 1,
                last_reason_code = 'network_timeout',
                last_detail = 'No response from provider'
            WHERE email = 'ada@example.com'
            """
        )
        connection.commit()
        summary_path, failure_path = sender.write_reports(
            connection, settings, stop_reason="network"
        )
        assert (os.stat(summary_path).st_mode & 0o777) == 0o600
        assert (os.stat(failure_path).st_mode & 0o777) == 0o600
        summary_text = summary_path.read_text()
        assert "ada@example.com" not in summary_text
        with failure_path.open(newline="") as handle:
            rows = list(csv.DictReader(handle))
        assert rows
        assert set(rows[0]) == {
            "email",
            "stage",
            "reason_code",
            "disposition",
            "detail",
        }
        assert any(
            row["email"] == "ada@example.com"
            and row["reason_code"] == "network_timeout"
            for row in rows
        )
    finally:
        connection.close()


def test_dry_run_preflights_but_never_sends(
    tmp_path: Path,
    fixture_csv: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = make_settings(tmp_path, fixture_csv)
    client = FakeSES()
    with caplog.at_level(logging.INFO):
        result = sender.execute(settings, client_factory=lambda _settings: client)
    assert result == 0
    assert client.send_calls == []
    assert "ada@example.com" not in caplog.text
    assert "Ada Lovelace" not in caplog.text


def test_live_execute_completes_with_fake_ses(
    tmp_path: Path,
    fixture_csv: Path,
) -> None:
    settings = make_settings(
        tmp_path,
        fixture_csv,
        send=True,
        confirm_opted_in=True,
    )
    client = FakeSES()
    result = sender.execute(settings, client_factory=lambda _settings: client)
    assert result == 0
    assert len(client.send_calls) == 1
    assert len(client.send_calls[0]["BulkEmailEntries"]) == 2
    summary_path, _failure_path = sender._artifact_paths(settings)
    summary = json.loads(summary_path.read_text())
    assert summary["complete"] is True
    assert summary["counts"]["accepted"] == 2
    assert summary["counts"]["input_duplicate"] == 1
    assert summary["counts"]["total"] == 8


def test_create_client_disables_sdk_retries(
    tmp_path: Path,
    fixture_csv: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_client(service: str, *, region_name: str, config: Any) -> object:
        captured.update(
            service=service,
            region=region_name,
            retries=config.retries,
            pool=config.max_pool_connections,
        )
        return object()

    monkeypatch.setattr(sender.boto3, "client", fake_client)
    settings = make_settings(tmp_path, fixture_csv, workers=12)
    sender.create_ses_client(settings)
    assert captured == {
        "service": "sesv2",
        "region": "us-east-1",
        "retries": {"mode": "standard", "total_max_attempts": 1},
        "pool": 12,
    }


def test_scheduler_never_exceeds_worker_concurrency(
    tmp_path: Path,
    fixture_csv: Path,
) -> None:
    class ConcurrencySES(FakeSES):
        def __init__(self) -> None:
            super().__init__()
            self.active = 0
            self.maximum = 0
            self.lock = threading.Lock()

        def send_bulk_email(self, **kwargs: Any) -> dict[str, Any]:
            with self.lock:
                self.active += 1
                self.maximum = max(self.maximum, self.active)
            try:
                time.sleep(0.005)
                return success_response(len(kwargs["BulkEmailEntries"]))
            finally:
                with self.lock:
                    self.active -= 1

    settings = make_settings(tmp_path, fixture_csv, workers=2)
    connection = sender.connect_state(settings.state_path)
    client = ConcurrencySES()
    try:
        connection.execute(
            "INSERT INTO campaign VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                settings.campaign_id,
                str(settings.input_path),
                "digest",
                settings.region,
                settings.from_email,
                settings.template_name,
                settings.configuration_set,
                sender.utc_now(),
            ),
        )
        _insert_pending(connection, settings.campaign_id, 250)
        result = sender.run_scheduler(
            connection,
            client,
            settings,
            send_budget=250,
            target_rate=10_000,
        )
        assert result.stop_reason is None
        assert client.maximum <= settings.workers
        assert result.max_outstanding <= 2 * settings.workers
        assert (
            sender.aggregate_counts(connection, settings.campaign_id)["accepted"] == 250
        )
    finally:
        connection.close()
