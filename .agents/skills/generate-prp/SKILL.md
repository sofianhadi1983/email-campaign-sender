---
name: generate-prp
description: Research a requested feature and create a complete, implementation-ready Product Requirement Prompt (PRP) from a repository feature file. Use when the user asks to generate, write, or prepare a PRP from feature requirements.
---

# Generate a PRP

Expect a path to a feature requirements file. If the path is missing or
ambiguous, ask the user for it before continuing.

1. Read the feature file completely.
2. Read the applicable `AGENTS.md` chain for the feature, the PRP output, and
   any source paths likely to be referenced.
3. Analyze the codebase:
   - Find similar features and implementation patterns.
   - Identify concrete files and symbols the implementer should inspect.
   - Record repository conventions and existing validation commands.
4. Research external sources only where they add necessary context:
   - Prefer official library documentation and primary sources.
   - Include direct URLs and the specific sections that matter.
   - Capture version-specific behavior, common pitfalls, and relevant examples.
5. Ask the user only when a missing decision would materially change the PRP.
6. Read `PRPs/templates/prp_base.md` and use it as the output structure.
7. Write an implementation blueprint that includes:
   - Pseudocode for the intended approach.
   - Ordered implementation tasks.
   - Existing repository files to follow.
   - Required error handling.
   - Executable validation gates that match this repository.
8. Save the result as `PRPs/{feature-name}.md`.
9. Re-read the feature file, template, and finished PRP. Confirm that all
   necessary context is present and every validation command is executable.
10. Report the output path and score confidence from 1–10 for successful
    one-pass implementation by Codex, with a brief reason for the score.

Do not begin implementing the feature. The output of this workflow is the PRP.
