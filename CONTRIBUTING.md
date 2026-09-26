# Contributing to northstar-api

## Branching

Use focused branches such as `feature/<scope>` or `fix/<scope>` from the current integration branch. Keep one product or release concern per branch.

## Validation

Run from this repository:

```powershell
uv run ruff check .
uv run ruff format --check .
uv run python -m pytest
uv lock --check
```

## Pull Requests

Describe the endpoint behavior changed, tests added, validation run, and any remaining release risk. Keep routers thin and avoid placing Domain or Application business logic in API code.

## Review Expectations

Reviewers check request validation, response-schema compatibility, HTTP error semantics, dependency direction, ASGI-level coverage, and accidental exposure of internal implementation details.
