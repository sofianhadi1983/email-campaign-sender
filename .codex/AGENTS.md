# Codex Configuration

## Purpose

- Store repository-local Codex permissions and configuration.

## Ownership

- `config.toml` owns the default repository permission profile.

## Local Contracts

- Keep filesystem access scoped to the active workspace.
- Keep command-line network access limited to domains required by this repository.
- Do not store credentials or OAuth tokens here.

## Work Guidance

- Preserve valid TOML in `config.toml`.

## Verification

- Parse `config.toml` with a TOML parser after editing it.

## Child DOX Index
