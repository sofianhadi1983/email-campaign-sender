---
name: execute-prp
description: Implement a repository feature from an existing Product Requirement Prompt (PRP), run its validation gates, and verify every requirement. Use when the user asks to execute, implement, or complete a specific PRP file.
---

# Execute a PRP

Expect a path to a PRP file. If the path is missing or ambiguous, ask the user
for it before continuing.

1. Read the PRP completely, including every referenced local file and
   applicable `AGENTS.md` chain.
2. Inspect the current worktree and relevant implementation patterns. Extend
   research only where the PRP lacks information required for implementation.
3. Convert the PRP into a concise plan with verifiable outcomes and track it
   through completion.
4. Implement every requirement with the smallest changes that fit the existing
   codebase.
5. Run each validation command specified by the PRP. Fix failures caused by the
   implementation and rerun until they pass.
6. Re-read the PRP and compare every requirement and checklist item against the
   finished implementation.
7. Run the final relevant validation suite and review the diff for unrelated
   changes.
8. Perform the required DOX closeout for all changed paths.
9. Report completed work, validation results, and any genuine unresolved
   blockers.

Do not declare completion while a PRP requirement or required validation gate
remains unaddressed.
