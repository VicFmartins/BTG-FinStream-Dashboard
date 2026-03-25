# Architecture Overview

`BTG FinStream Dashboard` is organized as a small monorepo with clear service boundaries:

- `frontend` renders the dashboard shell and calls the backend through `/api`.
- `backend` exposes HTTP routes, central settings, and the starting structure for services, schemas, and future WebSocket features.
- `producer` simulates live market ticks and publishes them to Redpanda.
- `postgres` stores relational data and can back aggregates or snapshots.
- `redis` is reserved for low-latency cache and pub/sub workloads.
- `redpanda` transports event streams across producers and consumers.

## Backend Layout

```text
app/
|-- api/
|   `-- routes/
|-- core/
|-- models/
|-- schemas/
|-- services/
|-- websocket/
`-- main.py
```

This layout keeps responsibilities separated without adding unnecessary abstraction yet:

- `api/routes` contains route handlers.
- `core` contains shared runtime concerns such as settings and CORS.
- `schemas` contains request and response models.
- `services` contains lightweight business logic helpers.
- `models` is reserved for persistence models.
- `websocket` is reserved for real-time delivery features.
