# Northstar API

`northstar-api` is the thin HTTP delivery layer for the Northstar Intelligence Alpha.

## Implemented Alpha API

- `POST /analyze`: analyzes one asset symbol and returns Recommendation, RecommendationExplanation, and MarketObservationContext.
- `POST /watchlist/refresh`: analyzes an ordered collection of symbols through the existing AnalyzeWatchlistUseCase and preserves partial results and stable failure codes.
- Request validation through Pydantic schemas.
- Generic transport-safe handling for unknown symbols, invalid requests, and application/provider failures.
- FastAPI package entry point via `uv run python -m northstar_api`.

The router delegates to Application workflows and serializes their results. It does not calculate recommendations, interpret market evidence, or communicate with providers directly.

## Dependencies

The API composes:

- `northstar-application` for use-case orchestration;
- `northstar-core` for Domain contracts and values;
- `northstar-infrastructure` for the Yahoo Finance observation adapter.

## Future API Capabilities

The following are not implemented in Alpha:

- authentication and authorization;
- portfolio endpoints;
- execution endpoints;
- notifications and alerts;
- opportunity ranking and Today's Opportunities;
- persistence and user-owned watchlists;
- rate limiting, metrics, and telemetry.

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md) for setup and validation commands. Contribution and review expectations are documented in [CONTRIBUTING.md](CONTRIBUTING.md).
