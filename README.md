# Amazon SES bulk campaign sender

`campaign_sender.py` submits one stored-template promotion to a large,
opted-in audience through the Amazon SES v2 `SendBulkEmail` API. It streams CSV
input into SQLite, sends one personalized destination per recipient in batches
of 50, meters the Region's recipient-per-second quota, and checkpoints every
outcome for safe manual resume.

SES acceptance means that SES accepted the message for delivery. It does not
mean that the message reached an inbox. Delivery, bounce, complaint, reject,
and rendering-failure outcomes belong to the SES configuration-set event
pipeline.

## Requirements

- Python 3.11 or newer, managed through `uv`.
- AWS credentials available through boto3's standard credential provider
  chain. Do not put access keys in command-line arguments or repository files.
- A dedicated SES account and Region with:
  - production access and current rolling daily capacity;
  - healthy, enabled sending;
  - a verified sending identity with DKIM configured;
  - an appropriate dedicated-IP warm-up plan when dedicated IPs are used;
  - account suppression enabled for both bounces and complaints;
  - a stored template containing `{{name}}`, text and HTML bodies, and a
    functional unsubscribe mechanism;
  - an enabled configuration set publishing `BOUNCE`, `COMPLAINT`, `DELIVERY`,
    `REJECT`, and `RENDERING_FAILURE`.
- A `name,email` CSV that was already filtered against the business
  unsubscribe list. Consent evidence remains in the source system.

Install the locked development environment:

```bash
uv sync --all-groups
```

## IAM

Grant the runtime identity only the actions it needs:

```text
ses:GetAccount
ses:GetEmailIdentity
ses:GetEmailTemplate
ses:TestRenderEmailTemplate
ses:GetConfigurationSet
ses:GetConfigurationSetEventDestinations
ses:ListSuppressedDestinations
ses:SendBulkEmail
```

Restrict the sending identity and configuration set with IAM resources and
conditions where SES supports them.

## Input

```csv
name,email
Ada Lovelace,ada@example.com
Grace Hopper,grace@example.com
```

The importer:

- streams rows and never loads the full audience into Python memory;
- normalizes and validates addresses without DNS lookups;
- records matching normalized email/name pairs as completed duplicates;
- marks malformed addresses and missing names as `bad_row`;
- excludes an address as `duplicate_conflict` when its rows disagree on name.

## Dry run

Dry run is the default. It imports the audience, loads the SES suppression
list, and checks every AWS prerequisite, but never calls `SendBulkEmail`:

```bash
uv run python campaign_sender.py \
  --input recipients.csv \
  --campaign-id july-2026-promotion \
  --region us-east-1 \
  --from-email promotions@example.com \
  --template-name july-2026-promotion \
  --configuration-set marketing-events \
  --state campaign-state.sqlite3
```

## Live run

Live sending requires both acknowledgements:

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

The script targets 90% of the current `MaxSendRate`. SES quotas count
recipients, so a full 50-destination request consumes 50 rate tokens. The 90%
target is an engineering margin, not an AWS guarantee. Explicit throttling
reduces the target; sustained successful batches recover gradually to 90%.

The theoretical submission floor is:

```text
recipient count / (0.90 × MaxSendRate)
```

At 1,000 recipients/second, one million recipients have a theoretical floor of
about 1,111 seconds. Network latency, throttling, list quality, daily quota, and
IP warm-up can increase the real time.

## Quota exhaustion and resume

The account is dedicated to this campaign, so the sender keeps no quota
reserve. If the rolling 24-hour quota cannot cover all pending recipients, the
script sends only within current capacity, checkpoints results, prints a
verbose reason, and exits nonzero. It does not wait for quota to replenish.
Run the same command later to continue.

The first stored campaign record and imported recipient set are authoritative.
On resume, changed values for the CSV, sender, Region, template, or
configuration set are ignored; the log lists only the ignored argument names.

Normal resume never resends:

- `accepted`, `bad_row`, `suppressed`, or `permanent_failure`;
- `retry_exhausted` without `--retry-exhausted`;
- `unknown` without an explicit duplicate-risk acknowledgement.

A definite retryable SES result receives at most three application
submissions: one initial attempt and two retries with capped full-jitter
backoff. Start a new three-attempt cycle explicitly:

```bash
uv run python campaign_sender.py ... \
  --send \
  --confirm-opted-in \
  --retry-exhausted
```

An `unknown` outcome means the request had no trustworthy response and SES
might already have accepted it. Retrying can duplicate the promotion:

```bash
uv run python campaign_sender.py ... \
  --send \
  --confirm-opted-in \
  --retry-unknown \
  --accept-duplicate-risk
```

The SDK itself performs one total attempt, so it cannot silently replay an
ambiguous timeout.

## State and reports

The SQLite state database and reports contain customer data. They are created
with mode `0600` and are ignored by Git.

- `campaign-summary-<campaign-id>.json` contains aggregate counts only.
- `failure-report-<campaign-id>.csv` contains full recipient addresses plus
  `stage`, `reason_code`, `disposition`, and sanitized `detail`.

Logs and aggregate summaries never include recipient names or full addresses.
The process exits successfully only after every row is `accepted`, `duplicate`,
`bad_row`, `suppressed`, or `permanent_failure`. Pending, retryable, in-flight,
retry-exhausted, and unknown work produces a nonzero exit.

## Validation

All project commands run through `uv`:

```bash
uv sync --all-groups
uv run ruff check .
uv run mypy campaign_sender.py
uv run pytest tests/ -v --cov=campaign_sender --cov-report=term-missing
uv run python campaign_sender.py --help
```

Unit tests use fake SES clients and require no AWS credentials or network.
Use SES mailbox-simulator addresses for a separately configured live
integration test; never use the million-recipient audience as a validation
step.
