# Product Requirement Prompts

## Purpose

- Store context-rich implementation prompts and the templates used to create them.

## Ownership

- PRP files in this directory describe feature goals, implementation context, and validation gates.
- Focused `*-research.md` notes preserve cited primary-source findings used by
  PRPs in this directory.
- `templates/` owns reusable PRP structure.

## Local Contracts

- PRPs must include measurable success criteria and executable validation steps.
- References to repository files and external documentation must state why they are needed.
- Research notes must identify their source files and distinguish documented
  behavior from engineering recommendations.

## Work Guidance

- Prefer concrete implementation guidance over generic advice.
- Keep example PRPs illustrative; do not treat them as current application code.

## Verification

- Check that referenced local paths and validation commands are internally consistent.
- Check that research-note citations resolve to preserved source documents.

## Child DOX Index

- [`templates/AGENTS.md`](templates/AGENTS.md) — reusable PRP templates.
