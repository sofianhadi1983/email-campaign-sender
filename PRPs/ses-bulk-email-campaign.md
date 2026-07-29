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
- Produces aggregate summaries plus a protected, detailed input/submission
  failure report.

“Finished” means every input row has a definite local outcome:
`accepted`, `bad_row`, `suppressed`, or `permanent_failure`. `unknown`,
`retryable`, `retry_exhausted`, `pending`, and `in_flight` remain incomplete
and produce a nonzero exit. Finished does not mean every message reached an
inbox.

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
  and an enabled configuration set with event publishing. The dedicated SES
  account does not need a campaign quota reserve.
- AWS credentials come from boto3's standard credential provider chain. No
  access keys are accepted as CLI arguments or stored in the repository.
- The script sends one destination email address per bulk entry. Do not combine
  customers in `To`, `Cc`, or `Bcc`.
- `SendBulkEmail` has no idempotency token. Durable checkpoints prevent normal
  reruns from duplicating accepted recipients, but exact-once behavior cannot
  be guaranteed if the client loses the response after SES accepted a request.
  Such exhausted/ambiguous network failures must become `unknown` and must not
  be retried automatically. The protected report must preserve a verbose,
  machine-readable reason for operator review.
- On first use, the imported recipients and campaign settings stored in SQLite
  become authoritative. A resume ignores later changes to the input path,
  sender, Region, template, and configuration set and warns that it is using
  the stored originals.
- If the remaining rolling quota cannot cover the campaign, submit only up to
  the currently available capacity, checkpoint all results, then exit nonzero
  with a verbose quota-exhaustion message. Do not wait inside the process.
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
      current daily capacity, and maximum send rate.
- [ ] The token bucket measures recipients, not API requests, and targets 90%
      of the current `MaxSendRate`; the documentation identifies 90% as an
      application engineering margin rather than an AWS requirement.
- [ ] Concurrency is bounded, the botocore connection pool is at least the
      worker count, and at most `2 * workers` requests are outstanding.
- [ ] Each `BulkEmailEntryResult` is mapped back to its exact recipient and
      persisted before more work is scheduled.
- [ ] Retryable per-entry statuses and explicit throttling responses use capped
      exponential backoff with full jitter and at most three total application
      submissions; permanent failures are not retried.
- [ ] A rerun uses the first stored campaign settings and imported recipient
      set, ignores changed bootstrap arguments, and skips recipients already
      marked `accepted`, `bad_row`, `suppressed`, `permanent_failure`,
      `retry_exhausted`, or `unknown` unless the operator supplies the matching
      explicit recovery acknowledgement.
- [ ] Live sending requires both `--send` and `--confirm-opted-in`; otherwise
      the command performs validation/preflight only.
- [ ] Exhausting the currently available rolling quota checkpoints progress,
      emits a verbose message, and exits nonzero without waiting.
- [ ] Exit code is zero only when no `pending`, `retryable`, `in_flight`,
      `retry_exhausted`, or `unknown` recipients remain.
- [ ] Logs do not print full customer names or email addresses.
- [ ] A protected `0600`, Git-ignored CSV reports full recipient addresses with
      `stage`, `reason_code`, `disposition`, and redacted `detail`; aggregate
      logs and summaries remain free of recipient PII.
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

- file: PRPs/ses-send-rate-utilization-research.md
  why: >
    Records the local-PDF evidence that MaxSendRate is recipient-based, short
    bursts are not sustained capacity, and the 90% target is an application
    engineering margin rather than AWS guidance.

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
  why: Understand which failures the SDK may retry automatically.
  critical: >
    Standard retries can replay ambiguous transport failures. Configure one
    total SDK attempt so the application can enforce the confirmed policy:
    retry only definite SES failures, and persist no-response failures as
    unknown without an automatic replay.

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
│   ├── ses-send-rate-utilization-research.md
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
# Disable SDK retries so a no-response transport failure is never replayed automatically.
# Do not issue one GetSuppressedDestination request per customer; that destroys throughput.
# Preload the account-level suppression list with its paginator, then rely on SES/global events.
# Sending to globally suppressed addresses can consume quota and affect bounce rate.
# Boto3's SES v2 client is synchronous; use bounded threads, not an unbounded task list.
# SQLite writes must remain on the coordinator thread to avoid lock contention.
# The SQLite state file contains PII: chmod 0600, do not commit it, and redact logs.
# The protected failure CSV contains addresses: chmod 0600, do not commit it, and never echo it.
# The recipient's name must be serialized with json.dumps; never build JSON with string interpolation.
# Non-ASCII local parts are unsupported by SES SMTPUTF8 rules; validate without DNS lookups.
# Daily quota is rolling. Submit only within current capacity, then checkpoint and exit.
# A short burst above MaxSendRate is not guaranteed sustained capacity; 90% is our margin, not AWS guidance.
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
    RETRY_EXHAUSTED = "retry_exhausted"
    ACCEPTED = "accepted"
    BAD_ROW = "bad_row"
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
    reason_code: str | None
    detail: str | None

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
    input_sha256 TEXT NOT NULL,
    region TEXT NOT NULL,
    from_email TEXT NOT NULL,
    template_name TEXT NOT NULL,
    configuration_set TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE recipient (
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

CREATE TABLE input_issue (
    campaign_id TEXT NOT NULL,
    row_number INTEGER NOT NULL,
    raw_email TEXT,
    status TEXT NOT NULL DEFAULT 'bad_row',
    reason_code TEXT NOT NULL,
    detail TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (campaign_id, row_number)
);

CREATE INDEX recipient_work
    ON recipient(campaign_id, status, email);
```

- Enable WAL mode.
- Set the state file mode to `0600`.
- On the first invocation, hash the CSV during the same streaming import pass
  and persist the effective campaign settings. On resume, load the stored
  settings and recipient set before considering bootstrap CLI values; warn
  about changed argument names, ignore their new values, and do not re-import
  the CSV.
- Insert recipients with chunked `executemany`. Silently deduplicate matching
  normalized email/name pairs. If one normalized email has conflicting names,
  mark it `bad_row` and record `duplicate_conflict` so neither version sends.
- Normalize the domain to IDNA and lowercase the comparison key. Reject
  addresses with a non-ASCII local part, control characters, missing names, or
  invalid structure. Do not perform per-address DNS lookups.
- Persist malformed/missing input as `bad_row` records in `input_issue`. Full
  raw addresses may appear only in the `0600` state database and protected
  failure CSV, never in logs or aggregate summaries.
- Use a stable failure taxonomy:
  - `stage`: `input` or `submission`.
  - Input `reason_code`: `missing_email`, `invalid_email`, `missing_name`, or
    `duplicate_conflict`.
  - Submission `reason_code`: normalized SES entry status, normalized provider
    exception category, `network_timeout`, or `response_length_mismatch`.
  - `disposition`: `bad_row`, `permanent`, `retryable`, `retry_exhausted`, or
    `unknown`.
  - `detail`: useful sanitized context without names, message bodies,
    credentials, request payloads, or duplicate copies of the address.

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
  - On first use, validate, normalize, hash, and import rows in chunks without
    DNS checks; persist malformed or missing values as bad_row input issues.
  - Silently deduplicate matching email/name pairs. Mark a normalized address
    bad_row with reason duplicate_conflict if its names disagree.
  - On resume, use the first stored campaign settings and imported recipients.
    Ignore later bootstrap values, warn using argument names only, and do not
    reopen or re-import the changed CSV.
  - Reset stale in_flight rows to unknown on startup, not retryable.
  - Load account-level suppressed destinations once through the SES v2 paginator
    and mark matching pending rows suppressed.

Task 3: Implement SES client and preflight
IN campaign_sender.py:
  - Build one boto3 sesv2 client with explicit region, standard retry mode,
    total_max_attempts=1, TCP keepalive, bounded timeouts, and a connection pool
    at least as large as the worker count.
  - Call get_account and require:
      ProductionAccessEnabled == true
      SendingEnabled == true
      EnforcementStatus == "HEALTHY"
      SendQuota.MaxSendRate > 0
      SuppressionAttributes contains BOUNCE and COMPLAINT
  - Compute the current recipient budget as pending recipients when
    Max24HourSend is unlimited, otherwise max(0, floor(Max24HourSend -
    SentLast24Hours)); do not require it to cover the whole campaign.
  - Verify the From identity is enabled for sending.
  - Fetch the stored template and require a {{name}} replacement tag plus both
    text and HTML content.
  - Render the template with safe sample JSON through TestRenderEmailTemplate.
  - Require the configuration set to exist and have an enabled event destination
    covering at least BOUNCE, COMPLAINT, DELIVERY, REJECT, and RENDERING_FAILURE.
  - Print total recipients, quota, target rate, request count, and estimated
    minimum submission time before live sending. If the current budget is zero,
    print a verbose quota-exhaustion reason and exit nonzero without sending.

Task 4: Implement quota-aware bulk scheduling
IN campaign_sender.py:
  - Create batches of exactly min(50, pending work, remaining recipient budget).
  - Build DefaultContent with TemplateName and fallback TemplateData.
  - Build one BulkEmailEntry per recipient using json.dumps({"name": name}).
  - Apply a campaign-id ReplacementTag without putting PII in tags.
  - Use a thread-safe token bucket measured in recipient tokens.
  - Set target_recipient_rate = 0.90 * MaxSendRate.
  - Acquire len(batch) tokens immediately before submitting the request.
  - Use ThreadPoolExecutor and keep no more than 2 * workers futures outstanding.
  - Keep all SQLite reads/writes and final status transitions on the coordinator
    thread.
  - Never schedule more recipients than the computed daily budget. When it is
    consumed, drain outstanding work, checkpoint, report remaining counts, and
    exit nonzero instead of waiting for quota to replenish.

Task 5: Implement result classification and retries
IN campaign_sender.py:
  - Zip response BulkEmailEntryResults to the submitted recipients; reject a
    response-length mismatch as unknown for the entire batch with a stable
    reason code and redacted detail.
  - Map SUCCESS to accepted and persist MessageId.
  - Retry ACCOUNT_THROTTLED, TRANSIENT_FAILURE, and FAILED with capped
    exponential full-jitter backoff and a maximum of three total application
    submissions: one initial call plus two retries.
  - After the third definite retryable result, mark the recipient
    retry_exhausted, continue other work, and require explicit operator action
    before another submission.
  - Treat ACCOUNT_DAILY_QUOTA_EXCEEDED as a global checkpoint-and-stop
    condition with a verbose nonzero exit.
  - Treat INVALID_PARAMETER, MESSAGE_REJECTED, and recipient-specific permanent
    failures as permanent_failure with stable reason codes.
  - Abort the whole run on account suspension, sending pause, missing template,
    missing configuration set, or unverified sending identity.
  - With SDK retries disabled, retry only explicit, definitive SES throttling
    or service responses within the three-submission cap. A transport/timeout
    failure without a response immediately marks every batch entry unknown with
    the exception category recorded as a reason code; continue other work and
    never replay those recipients automatically.
  - Reduce the target rate by 20% after explicit throttling and slowly recover
    toward 90% of MaxSendRate after sustained successful batches. Document both
    values as engineering policy, not AWS-prescribed behavior.

Task 6: Implement CLI safety, progress, and resume
IN campaign_sender.py:
  - Required arguments: input, campaign-id, region, from-email, template-name,
    configuration-set, and state.
  - When the state already contains campaign-id, resolve all effective settings
    and recipients from that first stored campaign. Ignore changed bootstrap
    values and log only the names of arguments that were ignored.
  - Optional tuning: workers with a conservative bounded default and target
    utilization capped at 0.90.
  - Optional recovery actions:
      --retry-exhausted explicitly resets retry_exhausted rows for a new
      three-submission cycle.
      --retry-unknown requires --accept-duplicate-risk and explicitly resets
      unknown rows despite possible prior SES acceptance.
    These flags are operational actions, not stored campaign settings.
  - Default mode imports, validates, and performs AWS preflight but does not send.
  - Require both --send and --confirm-opted-in for live traffic.
  - Log aggregate progress every five seconds: terminal/total, accepted,
    bad rows, failures, unknown, current recipients/sec, target rate, remaining
    daily budget, and elapsed time.
  - Never log names or full email addresses.
  - Write an atomic final JSON summary next to the state database.
  - Stream an atomic failure-report CSV with full email, stage, reason_code,
    disposition, and redacted detail. Set mode 0600 and never print its rows.
  - Handle SIGINT by stopping new scheduling, draining completed futures,
    persisting outcomes, and exiting nonzero.
  - Exit 0 only if no pending, retryable, in_flight, retry_exhausted, or unknown
    records remain. Quota exhaustion therefore exits nonzero while work waits.

Task 7: Add focused tests
CREATE tests/AGENTS.md:
  - Document test ownership and existing verification commands.

CREATE tests/fixtures/recipients.csv:
  - Include valid, identical duplicate, conflicting duplicate, Unicode-domain,
    invalid, and missing-field rows.

CREATE tests/test_campaign_sender.py:
  - Use botocore.stub.Stubber or a narrow fake SES client; never call AWS.
  - Test CSV streaming, validation, normalization, bad_row reporting, identical
    deduplication, and duplicate_conflict exclusion.
  - Test exact 50-entry batching and one recipient per destination.
  - Test template JSON escaping for quotes, slashes, and Unicode names.
  - Test recipient-based token accounting and bounded outstanding futures.
  - Test every BulkEmailEntryResult status classification.
  - Test the three-total-attempt cap, jitter injection with deterministic RNG,
    retry_exhausted, and global stop.
  - Test response-length mismatch and ambiguous exception -> verbose unknown
    without automatic retry; assert the SDK is configured for one total attempt.
  - Test current daily budget limits scheduling and quota exhaustion checkpoints
    before a verbose nonzero exit without sleeping for replenishment.
  - Test resume uses stored settings/recipients, ignores changed bootstrap
    values, and skips completed, retry_exhausted, and unknown rows.
  - Test --retry-exhausted is required to reset exhausted rows, and
    --retry-unknown cannot run without --accept-duplicate-risk.
  - Test dry-run safety and the two live-send acknowledgement flags.
  - Test the protected failure-report columns, full recipient address, 0600
    mode, and atomic replacement; test all logs and summaries redact PII.

Task 8: Document setup and operation
MODIFY README.md:
  - Replace the current one-line placeholder.
  - Document AWS credential-chain use and least-privilege IAM actions.
  - Document production access, quota increase, verified identity, DKIM,
    stored template, unsubscribe mechanism, suppression settings, and
    configuration-set event publishing prerequisites.
  - Explain input format, bad rows, dry run, live run, authoritative first-run
    state, changed-value ignore behavior, partial-quota exits, summaries,
    protected failure reports, retry exhaustion, and explicit unknown/retry
    recovery acknowledgements.
  - Include throughput examples using total / (0.90 * MaxSendRate).
  - State clearly that SES acceptance is not inbox delivery and exact-once
    delivery is not guaranteed.
```

### Per-task pseudocode

```python
def import_recipients(csv_path: Path, db: sqlite3.Connection, campaign_id: str) -> ImportStats:
    # First run only: stream and hash rows; never collect the whole file.
    # Validate with check_deliverability=False.
    # Buffer a small fixed number of tuples for executemany.
    # Record malformed/missing rows as bad_row input issues.
    # Deduplicate equal email/name pairs; exclude conflicting names.
    ...


def resolve_campaign(db: sqlite3.Connection, cli: Settings) -> Settings:
    # If campaign_id exists, return its first stored settings and recipients.
    # Warn with changed argument names only; ignore later values and CSV content.
    # Otherwise import once, then persist the effective settings and CSV hash.
    ...


def preflight(
    client: SESV2Client,
    pending_count: int,
    settings: Settings,
) -> tuple[AccountLimits, int]:
    account = client.get_account()
    assert account["ProductionAccessEnabled"]
    assert account["SendingEnabled"]
    assert account["EnforcementStatus"] == "HEALTHY"

    quota = account["SendQuota"]
    if quota["Max24HourSend"] == -1:
        send_budget = pending_count
    else:
        send_budget = max(
            0,
            min(pending_count, floor(
                quota["Max24HourSend"] - quota["SentLast24Hours"]
            )),
        )

    # Verify identity, template rendering, configuration set, event
    # destination, and BOUNCE+COMPLAINT suppression before any live send.
    return AccountLimits(...), send_budget


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
        # Reduce the shared limiter and retry only while attempts < 3.
        return retry_with_jitter(batch, max_total_attempts=3)
    except AMBIGUOUS_TRANSPORT_FAILURE as error:
        # SES may already have accepted some/all recipients.
        return mark_unknown(
            batch,
            reason_code=classify_exception(error),
            detail=redact_exception(error),
        )

    results = response["BulkEmailEntryResults"]
    if len(results) != len(batch):
        return mark_unknown(
            batch,
            reason_code="response_length_mismatch",
            detail="SES result count did not match the submitted batch",
        )

    return classify_each_entry(batch, results)


def run_scheduler() -> CampaignStats:
    # Coordinator owns SQLite.
    # Submit only while outstanding futures < 2 * workers.
    # Never schedule beyond the preflight recipient budget.
    # Persist returned outcomes before pulling/scheduling more rows.
    # On budget exhaustion drain, checkpoint, report, and exit nonzero.
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
  - The first stored campaign settings and recipient import are authoritative
    for every resume.

FAILURE_REPORT:
  - Atomic CSV beside the state database with mode 0600.
  - Columns: email, stage, reason_code, disposition, detail.
  - May contain full addresses; never log rows or include PII in summaries.
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

- CSV import streams, records malformed/missing values as bad_row, deduplicates
  equal rows, and excludes conflicting duplicate names.
- One million synthetic rows produce exactly 20,000 full 50-entry batches when
  all rows are unique, valid, and quota permits the entire run.
- Peak in-memory work queues remain bounded by chunk size and worker limits.
- A 50-recipient batch consumes 50 rate-limit tokens.
- JSON personalization correctly handles quotes and Unicode.
- Every documented SES per-entry result status has an explicit classification.
- An HTTP-success response with partial failures persists each entry separately.
- Explicit throttling reduces the limiter and retries with deterministic
  backoff; a recipient is submitted at most three times.
- Retry exhaustion is persisted, reported, and left for explicit operator
  action.
- Ambiguous network exhaustion becomes verbose unknown without application
  resubmission.
- A smaller daily budget sends only that many recipients, checkpoints pending
  work, reports quota exhaustion, and exits nonzero without waiting.
- Resume never resends completed or unknown recipients and uses the first
  stored settings/recipient import despite changed bootstrap values.
- The protected failure report has the exact schema and 0600 mode, while logs
  and the JSON summary contain no recipient PII.
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
      missing-events, and suppression-disabled states; zero current daily
      capacity produces a verbose nonzero exit, while partial capacity limits
      this invocation instead of blocking all work.
- [ ] Every SES per-entry result is checked and persisted.
- [ ] Definite retryable results receive no more than three total application
      submissions before becoming `retry_exhausted`.
- [ ] Ambiguous failures are visible and not automatically retried.
- [ ] Resume uses the first stored campaign settings and recipient import even
      when later bootstrap arguments change.
- [ ] No credentials, recipient CSVs, state databases, summaries, or failure
      reports are tracked.
- [ ] Logs, errors, and aggregate summaries redact recipient PII; only the
      protected state and failure report contain full addresses.
- [ ] README explains quota planning, opt-in/unsubscribe obligations,
      deliverability events, authoritative resume behavior, partial-quota exit,
      failure reports, three-attempt retry exhaustion, and exact-once
      limitations.
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
- Do not submit a recipient more than three application-level times in one
  automatic attempt cycle.
- Do not perform one suppression API call per recipient.
- Do not log PII, AWS credentials, or full SES request payloads.
- Do not expose the protected failure report or state database through logs.
- Do not wait in-process after current rolling quota is exhausted.
- Do not adopt changed bootstrap values when resuming a stored campaign.
- Do not start live sending without production access, current daily capacity,
  verified identity, event publishing, suppression, consent, and unsubscribe.

## Confidence Score: 9/10

High confidence in the SES v2 batching, quota, personalization, response, and
validation design because it is grounded in the downloaded AWS developer/API
references, the focused local rate-limit research note, and the confirmed
operational decisions from the grilling session. The remaining uncertainty is
operational rather than code-level: actual completion time depends on the
account's approved `MaxSendRate`, rolling daily quota, network latency,
IP/domain warm-up, list quality, and whether SES returns ambiguous transport
failures.
