name: "Quota-Aware Amazon SES Bulk Campaign Sender"
description: |
  Build a Python CLI that submits one personalized promotional campaign to
  1,000,000 opted-in recipients through the Amazon SES v2 SendBulkEmail API as
  quickly as the account's Region-specific quotas reasonably allow.

## Purpose

Provide an implementation-ready plan for a resumable, quota-aware campaign
sender that batches recipients efficiently, personalizes a stored SES template
with each recipient's name, and records every SES acceptance or failure.

## Core Principles

1. **SES quota is the speed limit**: maximize sustained utilization without
   intentionally exceeding `MaxSendRate`.
2. **Batch at the API maximum**: submit up to 50 one-recipient destinations per
   `SendBulkEmail` request.
3. **Bound all concurrency and memory**: stream the CSV, bound outstanding
   requests, and never load one million recipients into Python memory.
4. **Resume safely**: persist deduplication and per-recipient outcomes in
   SQLite.
5. **Treat acceptance accurately**: SES acceptance is not inbox delivery; use
   event publishing for downstream delivery, bounce, complaint, and rendering
   outcomes.
6. **Protect sender reputation**: send only to an opted-in, already-unsubscribed-
   filtered audience and honor SES suppression data.

---

## Goal

Create a Python 3.11+ command-line script that:

- Reads a CSV containing `name,email` rows for approximately 1,000,000
  customers.
- Deduplicates recipients by normalized email address.
- Uses an existing Amazon SES v2 stored template containing `{{name}}`.
- Sends unique personalized messages through `SendBulkEmail` in batches of at
  most 50 destinations.
- Drives submission at 90% of the current Region's `MaxSendRate` with bounded
  concurrency and adaptive backoff when SES throttles.
- Persists state so an interrupted campaign can resume without resubmitting
  recipients already recorded as accepted.
- Produces a machine-readable and human-readable summary.

“Finished” means every valid, non-suppressed recipient has a terminal local
state: `accepted`, `permanent_failure`, or `unknown`. It does not mean every
message reached an inbox.

## Why

- A sequential one-recipient loop would leave substantial SES throughput
  unused.
- Blind concurrency would exceed per-Region quotas, increase throttling, and
  risk account reputation.
- A one-million-recipient job needs durable progress, deduplication, bounded
  memory, and actionable failure reporting.
- Stored SES templates avoid regenerating the same subject and body 20,000
  times while still allowing per-recipient replacement data.

## What

The implementation is a single application module plus focused tests. A normal
live invocation should look like:

```bash
uv run python campaign_sender.py \
  --input recipients.csv \
  --campaign-id july-2026-promotion \
  --region us-east-1 \
  --from-email promotions@example.com \
  --template-name july-2026-promotion \
  --configuration-set marketing-events \
  --state campaign-state.sqlite3 \
  --send \
  --confirm-opted-in
```

The input contract is:

```csv
name,email
Ada Lovelace,ada@example.com
Grace Hopper,grace@example.com
```

### Assumptions and boundaries

- The audience has affirmatively opted in and has already been filtered against
  the business's unsubscribe list. The script must not invent consent.
- The SES stored template already contains a working unsubscribe mechanism and
  both HTML and text content appropriate for a promotional message.
- The selected SES Region has production access, a verified sending identity,
  an enabled configuration set with event publishing, and enough rolling
  24-hour quota for all pending recipients.
- AWS credentials come from boto3's standard credential provider chain. No
  access keys are accepted as CLI arguments or stored in the repository.
- The script sends one destination email address per bulk entry. Do not combine
  customers in `To`, `Cc`, or `Bcc`.
- `SendBulkEmail` has no idempotency token. Durable checkpoints prevent normal
  reruns from duplicating accepted recipients, but exact-once behavior cannot
  be guaranteed if the client loses the response after SES accepted a request.
  Such exhausted/ambiguous network failures must become `unknown` and must not
  be retried automatically.
- Creating SES identities, requesting production access/quota increases,
  creating the stored template, and provisioning SNS/EventBridge consumers are
  documented prerequisites, not responsibilities of this script.

### Success Criteria

- [ ] A 1,000,000-row CSV is processed with bounded Python memory; recipients
      are imported into SQLite in chunks rather than accumulated in a list.
- [ ] Duplicate normalized email addresses are sent at most once during normal
      operation.
- [ ] Every `SendBulkEmail` call contains 1–50 `BulkEmailEntry` objects and each
      object has exactly one `ToAddresses` recipient.
- [ ] Every entry supplies JSON `ReplacementTemplateData` containing the
      recipient's name.
- [ ] The sender performs preflight checks for production access, sending
      enabled status, healthy enforcement status, verified sender, stored
      template, configuration set event destination, suppression configuration,
      daily quota, and maximum send rate.
- [ ] The token bucket measures recipients, not API requests, and targets 90%
      of the current `MaxSendRate`.
- [ ] Concurrency is bounded, the botocore connection pool is at least the
      worker count, and at most `2 * workers` requests are outstanding.
- [ ] Each `BulkEmailEntryResult` is mapped back to its exact recipient and
      persisted before more work is scheduled.
- [ ] Retryable per-entry statuses and explicit throttling responses use capped
      exponential backoff with full jitter; permanent failures are not retried.
- [ ] A rerun with the same campaign ID and state database skips recipients
      already marked `accepted` or `permanent_failure`.
- [ ] Live sending requires both `--send` and `--confirm-opted-in`; otherwise
      the command performs validation/preflight only.
- [ ] Exit code is zero only when no `pending`, `retryable`, or `unknown`
      recipients remain.
- [ ] Logs do not print full customer names or email addresses.
- [ ] Unit tests pass without AWS credentials or network access.

## All Needed Context

### Documentation & References

```yaml
# Local snapshots downloaded from the official AWS documentation URLs.
- file: examples/ses-dg.pdf
  sections:
    - "PDF pages 21-24: Region-specific sending, recipient, and template quotas"
    - "PDF pages 61-67: rolling daily quota, maximum send rate, monitoring, and throttling"
    - "PDF pages 130-139: stored/inline templates and personalized SendBulkEmail examples"
  why: >
    Establishes the 50-destination bulk limit, recipient-based quotas,
    production-access requirements, and personalization workflow.
  critical: >
    A bulk call can include at most 50 destination objects; the number accepted
    can still be limited by MaxSendRate. Invalid template data can be accepted
    initially and later produce Rendering Failure events.

- file: examples/ses-apiv2.pdf
  sections:
    - "PDF pages 369-375: SendBulkEmail request, response, and operation errors"
    - "PDF pages 444-447: BulkEmailEntry and BulkEmailEntryResult statuses"
    - "PDF page 570: SendQuota fields"
  why: >
    Defines the exact SES v2 payload, per-entry response mapping, retryable
    statuses, and quota values.
  critical: >
    HTTP 200 is not enough. Inspect every BulkEmailEntryResult and persist each
    recipient's status independently.

- file: examples/ses-api.pdf
  why: >
    SES v1 API reference retained for comparison only.
  critical: >
    Do not implement the v1 SendBulkTemplatedEmail operation; this feature
    targets the SES v2 client and SendBulkEmail.

- file: examples/SHA256SUMS
  why: Integrity checks for the downloaded AWS PDF snapshots.

- url: https://docs.aws.amazon.com/boto3/latest/reference/services/sesv2/client/send_bulk_email.html
  section: "Request Syntax, BulkEmailEntries, and Response Structure"
  why: Current boto3 parameter names, result statuses, and exceptions.

- url: https://docs.aws.amazon.com/boto3/latest/reference/services/sesv2/client/get_account.html
  section: "Response Structure"
  why: >
    Read ProductionAccessEnabled, SendingEnabled, EnforcementStatus,
    Max24HourSend, MaxSendRate, SentLast24Hours, and suppression reasons.

- url: https://docs.aws.amazon.com/botocore/latest/reference/config.html
  section: "botocore.config.Config"
  why: Configure retries, timeouts, TCP keepalive, and max_pool_connections.

- url: https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html
  section: "Standard mode and retry behavior"
  why: Use SDK-standard exponential backoff and jitter for transient failures.
  critical: >
    Standard retries can retry ambiguous transport failures. If all SDK
    attempts fail without a definitive SES response, persist the batch as
    unknown and do not add another application-level retry.

- url: https://docs.aws.amazon.com/ses/latest/dg/quotas.html
  section: "Email sending quotas"
  why: Current Region-specific, recipient-based quota definitions.

- url: https://docs.aws.amazon.com/ses/latest/dg/manage-sending-quotas.html
  section: "Sending quota and sending rate"
  why: Rolling 24-hour quota behavior and production quota planning.

- url: https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications.html
  section: "Important considerations"
  why: Bounce and complaint notification requirements.

- url: https://docs.aws.amazon.com/ses/latest/dg/event-publishing-add-event-destination.html
  section: "Step 2: Add an event destination"
  why: Required configuration-set event publishing destinations.

- url: https://docs.aws.amazon.com/ses/latest/dg/event-publishing-retrieving-sns-contents.html
  section: "Event types and Rendering Failure object"
  why: Distinguish SES API acceptance from delivery and rendering outcomes.

- url: https://docs.aws.amazon.com/ses/latest/dg/sending-email-global-suppression-list.html
  section: "Global suppression list considerations"
  why: >
    Suppressed sends can still consume daily quota and affect bounce rate; the
    source audience must be clean and account suppression must be enabled.

- url: https://docs.aws.amazon.com/ses/latest/dg/send-an-email-from-console.html
  section: "Using the mailbox simulator manually"
  why: >
    Safe integration and throughput tests using labeled simulator addresses
    without consuming daily sending quota or reputation metrics.

- file: AGENTS.md
  why: Repository-wide implementation and DOX constraints.

- file: PRPs/AGENTS.md
  why: PRP success-criteria and executable-validation contract.
```

### Current Codebase Tree

```text
.
├── .agents/
│   └── skills/
├── .codex/
│   ├── AGENTS.md
│   └── config.toml
├── examples/
│   ├── AGENTS.md
│   ├── SHA256SUMS
│   ├── ses-api.pdf
│   ├── ses-apiv2.pdf
│   └── ses-dg.pdf
├── PRPs/
│   ├── AGENTS.md
│   ├── EXAMPLE_multi_agent_prp.md
│   ├── ses-bulk-email-campaign.md
│   └── templates/
│       ├── AGENTS.md
│       └── prp_base.md
├── .gitignore
├── AGENTS.md
├── README.md
└── SESSION_PROMPTS.md
```

There is no application implementation to mirror. Follow the repository's
root-level simplicity and surgical-change rules rather than the unrelated
multi-agent example PRP.

### Desired Codebase Tree

```text
.
├── campaign_sender.py              # CLI, CSV import, SQLite state, SES gateway, scheduler
├── pyproject.toml                  # Python metadata and pinned dependency ranges
├── README.md                       # SES prerequisites, setup, safety, usage, resume semantics
├── tests/
│   ├── AGENTS.md                   # Test ownership and verification contract
│   ├── fixtures/
│   │   └── recipients.csv          # Small deterministic input fixture
│   └── test_campaign_sender.py     # Unit and mocked integration tests
├── examples/                       # Existing official AWS source documents
└── PRPs/                           # Existing implementation prompts
```

Do not split the application into a package unless the single module becomes
genuinely difficult to test. The requested product is one script.

### Known Gotchas and Library Quirks

```python
# SES quotas are scoped per AWS Region and count recipients, not API calls.
# Sandbox quota is only 200 emails/day and 1 email/second; production access is mandatory.
# One SendBulkEmail call supports at most 50 destination objects.
# Use exactly one recipient per Destination to preserve privacy and result mapping.
# A successful HTTP response contains one result per entry; inspect all statuses.
# SUCCESS means SES accepted the message and will attempt delivery, not that it reached an inbox.
# Personalized template data can fail rendering after API acceptance; event publishing is required.
# SES has no idempotency token for SendBulkEmail; exact-once cannot be promised after ambiguous failures.
# Do not issue one GetSuppressedDestination request per customer; that destroys throughput.
# Preload the account-level suppression list with its paginator, then rely on SES/global events.
# Sending to globally suppressed addresses can consume quota and affect bounce rate.
# Boto3's SES v2 client is synchronous; use bounded threads, not an unbounded task list.
# SQLite writes must remain on the coordinator thread to avoid lock contention.
# The SQLite state file contains PII: chmod 0600, do not commit it, and redact logs.
# The recipient's name must be serialized with json.dumps; never build JSON with string interpolation.
# Non-ASCII local parts are unsupported by SES SMTPUTF8 rules; validate without DNS lookups.
# Daily quota is rolling. Max24HourSend - SentLast24Hours must cover pending recipients.
# Do not start a million-recipient run on a cold dedicated IP without an approved warm-up plan.
```

## Implementation Blueprint

### Data models and state

Keep the data structures in `campaign_sender.py`:

```python
class RecipientStatus(StrEnum):
    PENDING = "pending"
    IN_FLIGHT = "in_flight"
    RETRYABLE = "retryable"
    ACCEPTED = "accepted"
    PERMANENT_FAILURE = "permanent_failure"
    UNKNOWN = "unknown"
    SUPPRESSED = "suppressed"

@dataclass(frozen=True)
class Recipient:
    email: str
    name: str

@dataclass(frozen=True)
class EntryOutcome:
    email: str
    status: RecipientStatus
    message_id: str | None
    error: str | None

@dataclass(frozen=True)
class AccountLimits:
    max_24_hour_send: float
    sent_last_24_hours: float
    max_send_rate: float
```

SQLite schema:

```sql
CREATE TABLE campaign (
    campaign_id TEXT PRIMARY KEY,
    input_path TEXT NOT NULL,
    region TEXT NOT NULL,
    template_name TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE recipient (
    campaign_id TEXT NOT NULL,
    email TEXT NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    message_id TEXT,
    last_error TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (campaign_id, email)
);

CREATE INDEX recipient_work
    ON recipient(campaign_id, status, email);
```

- Enable WAL mode.
- Set the state file mode to `0600`.
- Insert recipients with chunked `executemany` and
  `ON CONFLICT(campaign_id, email) DO NOTHING`.
- Normalize the domain to IDNA and lowercase the comparison key. Reject
  addresses with a non-ASCII local part, control characters, missing names, or
  invalid structure. Do not perform per-address DNS lookups.
- Record invalid input counts without storing or logging full rejected values.

### Ordered implementation tasks

```yaml
Task 1: Establish the Python project
CREATE pyproject.toml:
  - Require Python >=3.11.
  - Runtime dependencies: boto3 and email-validator.
  - Development dependencies: pytest, pytest-cov, ruff, mypy.
  - Configure ruff and mypy for campaign_sender.py and tests.

MODIFY .gitignore:
  - Ignore *.sqlite3, *.sqlite3-shm, *.sqlite3-wal, campaign-summary*.json,
    recipients*.csv, and failure-report*.csv.
  - Preserve existing unrelated ignore rules.

Task 2: Implement input import and durable state
CREATE campaign_sender.py:
  - Parse CSV with csv.DictReader and require exactly usable name/email fields.
  - Validate and normalize rows without DNS checks.
  - Import in chunks and deduplicate through the SQLite primary key.
  - Refuse to reuse a campaign_id with different region/template/input metadata.
  - Reset stale in_flight rows to unknown on startup, not retryable.
  - Load account-level suppressed destinations once through the SES v2 paginator
    and mark matching pending rows suppressed.

Task 3: Implement SES client and preflight
IN campaign_sender.py:
  - Build one boto3 sesv2 client with explicit region, standard retry mode,
    total_max_attempts=3, TCP keepalive, bounded timeouts, and a connection pool
    at least as large as the worker count.
  - Call get_account and require:
      ProductionAccessEnabled == true
      SendingEnabled == true
      EnforcementStatus == "HEALTHY"
      SendQuota.MaxSendRate > 0
      remaining daily quota >= count(pending + retryable)
      SuppressionAttributes contains BOUNCE and COMPLAINT
  - Verify the From identity is enabled for sending.
  - Fetch the stored template and require a {{name}} replacement tag plus both
    text and HTML content.
  - Render the template with safe sample JSON through TestRenderEmailTemplate.
  - Require the configuration set to exist and have an enabled event destination
    covering at least BOUNCE, COMPLAINT, DELIVERY, REJECT, and RENDERING_FAILURE.
  - Print total recipients, quota, target rate, request count, and estimated
    minimum submission time before live sending.

Task 4: Implement quota-aware bulk scheduling
IN campaign_sender.py:
  - Create batches of exactly min(50, remaining) recipients.
  - Build DefaultContent with TemplateName and fallback TemplateData.
  - Build one BulkEmailEntry per recipient using json.dumps({"name": name}).
  - Apply a campaign-id ReplacementTag without putting PII in tags.
  - Use a thread-safe token bucket measured in recipient tokens.
  - Set target_recipient_rate = 0.90 * MaxSendRate.
  - Acquire len(batch) tokens immediately before submitting the request.
  - Use ThreadPoolExecutor and keep no more than 2 * workers futures outstanding.
  - Keep all SQLite reads/writes and final status transitions on the coordinator
    thread.

Task 5: Implement result classification and retries
IN campaign_sender.py:
  - Zip response BulkEmailEntryResults to the submitted recipients; reject a
    response-length mismatch as unknown for the entire batch.
  - Map SUCCESS to accepted and persist MessageId.
  - Retry ACCOUNT_THROTTLED, TRANSIENT_FAILURE, and FAILED with capped
    exponential full-jitter backoff and a maximum of five application attempts.
  - Treat ACCOUNT_DAILY_QUOTA_EXCEEDED as a global stop condition.
  - Treat INVALID_PARAMETER, MESSAGE_REJECTED, and recipient-specific permanent
    failures as permanent_failure.
  - Abort the whole run on account suspension, sending pause, missing template,
    missing configuration set, or unverified sending identity.
  - Allow the SDK to handle request-level retryable errors. If the SDK still
    returns an ambiguous transport/timeout failure, mark every batch entry
    unknown and do not retry automatically.
  - Reduce the target rate by 20% after explicit throttling and slowly recover
    toward 90% of MaxSendRate after sustained successful batches.

Task 6: Implement CLI safety, progress, and resume
IN campaign_sender.py:
  - Required arguments: input, campaign-id, region, from-email, template-name,
    configuration-set, and state.
  - Optional tuning: workers with a conservative bounded default and target
    utilization capped at 0.90.
  - Default mode imports, validates, and performs AWS preflight but does not send.
  - Require both --send and --confirm-opted-in for live traffic.
  - Log aggregate progress every five seconds: terminal/total, accepted,
    failures, unknown, current recipients/sec, target rate, and elapsed time.
  - Never log names or full email addresses.
  - Write an atomic final JSON summary next to the state database.
  - Handle SIGINT by stopping new scheduling, draining completed futures,
    persisting outcomes, and exiting nonzero.
  - Exit 0 only if no pending, retryable, in_flight, or unknown records remain.

Task 7: Add focused tests
CREATE tests/AGENTS.md:
  - Document test ownership and existing verification commands.

CREATE tests/fixtures/recipients.csv:
  - Include valid, duplicate, Unicode-domain, invalid, and missing-field rows.

CREATE tests/test_campaign_sender.py:
  - Use botocore.stub.Stubber or a narrow fake SES client; never call AWS.
  - Test CSV streaming, validation, normalization, and deduplication.
  - Test exact 50-entry batching and one recipient per destination.
  - Test template JSON escaping for quotes, slashes, and Unicode names.
  - Test recipient-based token accounting and bounded outstanding futures.
  - Test every BulkEmailEntryResult status classification.
  - Test retry cap, jitter injection with deterministic RNG, and global stop.
  - Test response-length mismatch and ambiguous exception -> unknown.
  - Test resume skips accepted/permanent/suppressed and does not retry unknown.
  - Test dry-run safety and the two live-send acknowledgement flags.
  - Test all logs redact recipient PII.

Task 8: Document setup and operation
MODIFY README.md:
  - Replace the current one-line placeholder.
  - Document AWS credential-chain use and least-privilege IAM actions.
  - Document production access, quota increase, verified identity, DKIM,
    stored template, unsubscribe mechanism, suppression settings, and
    configuration-set event publishing prerequisites.
  - Explain input format, dry run, live run, resume, summary, and unknown-state
    handling.
  - Include throughput examples using total / (0.90 * MaxSendRate).
  - State clearly that SES acceptance is not inbox delivery and exact-once
    delivery is not guaranteed.
```

### Per-task pseudocode

```python
def import_recipients(csv_path: Path, db: sqlite3.Connection, campaign_id: str) -> ImportStats:
    # Stream rows; never collect the whole file.
    # Validate with check_deliverability=False.
    # Buffer a small fixed number of tuples for executemany.
    # Let SQLite's composite primary key provide durable deduplication.
    ...


def preflight(client: SESV2Client, pending_count: int, settings: Settings) -> AccountLimits:
    account = client.get_account()
    assert account["ProductionAccessEnabled"]
    assert account["SendingEnabled"]
    assert account["EnforcementStatus"] == "HEALTHY"

    quota = account["SendQuota"]
    remaining = quota["Max24HourSend"] - quota["SentLast24Hours"]
    if quota["Max24HourSend"] != -1 and remaining < pending_count:
        raise PreflightError("Insufficient rolling 24-hour quota")

    # Verify identity, template rendering, configuration set, event
    # destination, and BOUNCE+COMPLAINT suppression before any live send.
    return AccountLimits(...)


def build_bulk_request(batch: Sequence[Recipient], settings: Settings) -> dict[str, object]:
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


def submit_batch(batch: list[Recipient]) -> BatchOutcome:
    rate_limiter.acquire(tokens=len(batch))
    try:
        response = ses.send_bulk_email(**build_bulk_request(batch, settings))
    except EXPLICIT_NON_AMBIGUOUS_THROTTLING:
        return retry_with_jitter(batch)
    except AMBIGUOUS_TRANSPORT_FAILURE as error:
        # SES may already have accepted some/all recipients.
        return mark_unknown(batch, error)

    results = response["BulkEmailEntryResults"]
    if len(results) != len(batch):
        return mark_unknown(batch, "SES result count mismatch")

    return classify_each_entry(batch, results)


def run_scheduler() -> CampaignStats:
    # Coordinator owns SQLite.
    # Submit only while outstanding futures < 2 * workers.
    # Persist returned outcomes before pulling/scheduling more rows.
    # On SIGINT stop producing, drain completed work, commit, and exit nonzero.
    ...
```

### Integration Points

```yaml
AWS_SES_V2:
  client: "boto3.client('sesv2', region_name=..., config=Config(...))"
  operations:
    - GetAccount
    - GetEmailIdentity
    - GetEmailTemplate
    - TestRenderEmailTemplate
    - GetConfigurationSet
    - GetConfigurationSetEventDestinations
    - ListSuppressedDestinations
    - SendBulkEmail

IAM:
  - Grant only the read operations above plus ses:SendBulkEmail.
  - Restrict sending identity and configuration set where IAM condition/resource
    support permits.

SES_TEMPLATE:
  - Stored in the same Region as the campaign.
  - Contains {{name}} in personalized content.
  - Contains text and HTML versions and a functional unsubscribe mechanism.

SES_CONFIGURATION_SET:
  - Passed on every bulk request.
  - Publishes bounce, complaint, delivery, reject, and rendering-failure events.

LOCAL_STATE:
  - SQLite file is campaign-specific durable PII.
  - File mode 0600 and gitignored.
  - Main thread is the only writer.
```

## Validation Loop

### Level 1: Dependency, syntax, style, and types

```bash
uv sync --all-groups
uv run ruff check .
uv run mypy campaign_sender.py
```

Expected: all commands exit 0. Do not use `--fix` in the final validation run.

### Level 2: Unit and mocked integration tests

```bash
uv run pytest tests/ -v --cov=campaign_sender --cov-report=term-missing
```

Required test cases:

- CSV import streams and deduplicates.
- One million synthetic rows produce exactly 20,000 full 50-entry batches when
  all rows are unique and valid.
- Peak in-memory work queues remain bounded by chunk size and worker limits.
- A 50-recipient batch consumes 50 rate-limit tokens.
- JSON personalization correctly handles quotes and Unicode.
- Every documented SES per-entry result status has an explicit classification.
- An HTTP-success response with partial failures persists each entry separately.
- Explicit throttling retries with deterministic backoff in tests.
- Ambiguous network exhaustion becomes unknown without application resubmission.
- Resume never resends accepted recipients.
- Live mode cannot run without both safety flags.

### Level 3: Local dry run

```bash
uv run python campaign_sender.py \
  --input tests/fixtures/recipients.csv \
  --campaign-id local-validation \
  --region us-east-1 \
  --from-email sender@example.com \
  --template-name test-template \
  --configuration-set test-events \
  --state /tmp/email-campaign-local-validation.sqlite3
```

Expected:

- No `SendBulkEmail` call occurs.
- Input and local state validation complete.
- Missing AWS credentials or SES prerequisites produce a concise preflight
  error and nonzero exit rather than sending.

### Level 4: AWS mailbox-simulator integration

Generate unique labeled simulator destinations so SQLite deduplication does not
collapse the test:

```bash
uv run python -c \
  'import csv; f=open("/tmp/ses-simulator.csv","w",newline=""); w=csv.writer(f); w.writerow(["name","email"]); [w.writerow([f"Test {i}",f"success+campaign-{i}@simulator.amazonses.com"]) for i in range(1000)]; f.close()'
```

Then run only in a configured non-production test campaign:

```bash
uv run python campaign_sender.py \
  --input /tmp/ses-simulator.csv \
  --campaign-id simulator-throughput-validation \
  --region us-east-1 \
  --from-email VERIFIED_SENDER \
  --template-name VERIFIED_TEMPLATE \
  --configuration-set VERIFIED_CONFIGURATION_SET \
  --state /tmp/ses-simulator.sqlite3 \
  --send \
  --confirm-opted-in
```

Expected:

- All 1,000 entries reach `accepted`.
- No request contains more than 50 entries.
- Observed steady-state acceptance rate approaches but does not sustainably
  exceed 90% of `MaxSendRate`.
- Configuration-set events contain delivery records and campaign tags.

Do not run a one-million-recipient live campaign as a validation step.

## Final Validation Checklist

- [ ] `uv sync --all-groups` succeeds from a clean checkout.
- [ ] `uv run ruff check .` passes.
- [ ] `uv run mypy campaign_sender.py` passes.
- [ ] `uv run pytest tests/ -v --cov=campaign_sender --cov-report=term-missing`
      passes.
- [ ] Tests prove 50-entry batching, recipient-token accounting, bounded
      concurrency, and durable resume.
- [ ] Dry-run is the default and cannot call `SendBulkEmail`.
- [ ] Live sending requires both explicit safety flags.
- [ ] Preflight blocks sandbox, unhealthy, paused, unverified, missing-template,
      missing-events, suppression-disabled, and insufficient-quota states.
- [ ] Every SES per-entry result is checked and persisted.
- [ ] Ambiguous failures are visible and not automatically retried.
- [ ] No credentials, recipient CSVs, state databases, or summaries are tracked.
- [ ] Logs and errors redact recipient PII.
- [ ] README explains quota planning, opt-in/unsubscribe obligations,
      deliverability events, resume behavior, and exact-once limitations.
- [ ] All changed paths receive the required DOX closeout.

---

## Anti-Patterns to Avoid

- Do not use raw SMTP or call `SendEmail` once per recipient.
- Do not use SES v1 `SendBulkTemplatedEmail`.
- Do not submit more than 50 destination objects in one request.
- Do not place multiple customers in one destination's To/Cc/Bcc fields.
- Do not hardcode a request-per-second rate; quotas count recipients.
- Do not launch one future per recipient or one future per all 20,000 batches.
- Do not share SQLite writes across worker threads.
- Do not decide success from the HTTP status alone.
- Do not retry permanent per-entry failures.
- Do not automatically retry an ambiguous exhausted network failure.
- Do not perform one suppression API call per recipient.
- Do not log PII, AWS credentials, or full SES request payloads.
- Do not start live sending without production access, enough daily quota,
  verified identity, event publishing, suppression, consent, and unsubscribe.

## Confidence Score: 8/10

High confidence in the SES v2 batching, quota, personalization, response, and
validation design because it is grounded in the downloaded AWS developer and
API references plus current boto3 documentation. The remaining uncertainty is
operational rather than code-level: actual completion time depends on the
account's approved `MaxSendRate`, rolling daily quota, network latency, IP/domain
warm-up, list quality, and whether SES returns ambiguous transport failures.
