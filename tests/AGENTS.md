# Campaign Sender Tests

## Purpose

- Verify input handling, durable state, SES request construction, scheduling,
  retries, reporting, and CLI safety without network access.

## Ownership

- `test_campaign_sender.py` owns unit and mocked integration coverage.
- `fixtures/` owns deterministic, synthetic recipient inputs.

## Local Contracts

- Tests must not use real AWS credentials or make network requests.
- Recipient fixtures must use synthetic addresses and names.
- Assertions involving PII must inspect protected artifacts, not emitted logs.

## Work Guidance

- Prefer narrow fake SES clients and deterministic clocks/random sources.

## Verification

- Run `uv run pytest tests/ -v --cov=campaign_sender --cov-report=term-missing`.

## Child DOX Index

