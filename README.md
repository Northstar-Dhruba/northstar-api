# Northstar API

Northstar API is the orchestration layer of the Northstar platform.

It exposes REST APIs, schedules jobs, communicates with external services, manages persistence, and coordinates all platform components.

---

## Responsibilities

- REST APIs
- Authentication
- Scheduler
- Database
- Notifications
- Market Data Collection
- Broker Integrations
- Logging
- Configuration
- User Management

---

## Non-Responsibilities

Northstar API does **not** contain:

- Trading Logic
- Technical Indicators
- Strategy Calculations
- Risk Algorithms
- Backtesting Logic

These belong inside **northstar-core**.

---

## Design Principles

- Thin API Layer
- Dependency Injection
- Modular Services
- Clean Architecture
- Infrastructure Layer

---

## Repository Structure

```
app/
tests/
scripts/
```

---

## Future Integrations

- Yahoo Finance
- Polygon.io
- NSE
- Zerodha
- WhatsApp
- Telegram
- Email

---

## License

Private