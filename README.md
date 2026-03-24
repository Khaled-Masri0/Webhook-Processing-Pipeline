# Webhook-Driven Task Processing Pipeline

Webhook-driven automation service built with TypeScript, Express, PostgreSQL, Prisma, Docker Compose, and GitHub Actions. It accepts inbound webhooks, queues them as jobs, processes them in a background worker, and delivers the processed result to registered subscriber endpoints.

## Features

- Pipeline CRUD API
- Webhook ingestion with asynchronous job creation
- Background worker with database-backed job queue
- Three processing action types: `TRANSFORM`, `FILTER`, `ENRICH`
- Processing retry logic with delayed requeue
- Subscriber delivery fan-out with retry handling
- Job status, history, and delivery-attempt APIs
- Docker Compose setup for API, worker, and PostgreSQL
- GitHub Actions CI for lint, build, test, and Docker stack validation

## Architecture

High-level flow:

1. A client creates a pipeline with a unique `sourcePath`, action config, and subscribers.
2. A webhook is sent to that `sourcePath`.
3. The API stores the payload as a `PENDING` job and returns `202 Accepted`.
4. The worker claims the next ready job from PostgreSQL.
5. The worker executes the configured action.
6. Successful results are delivered to active subscribers.
7. Failed processing or failed deliveries are retried with delayed backoff.
8. Operators can query job history and delivery history through read APIs.

Runtime layout:

- `api`: Express HTTP service for pipelines, webhooks, health checks, and job queries
- `worker`: background processor for queued jobs and delivery retries
- `db`: PostgreSQL for persistence and queue state

## Tech Stack

- TypeScript
- Express
- PostgreSQL
- Prisma
- Node.js
- Docker Compose
- GitHub Actions

## Design Choices

- PostgreSQL is used as both the primary datastore and the first queue implementation.
- API handling and background processing are separated into different services.
- `api` and `worker` run in separate containers but share the same application image.
- Job history and delivery history use pagination to keep read APIs predictable as data grows.
- Retries are asynchronous only. The API returns quickly and never waits for processing or subscriber delivery to finish.

## API

Health:

- `GET /health`

Pipeline management:

- `GET /pipelines` list pipelines
- `POST /pipelines` create a pipeline
- `GET /pipelines/:id` get one pipeline
- `PUT /pipelines/:id` update a pipeline
- `DELETE /pipelines/:id` delete a pipeline

Webhook ingestion:

- `POST <pipeline.sourcePath>`
- Example: `POST /webhooks/order-events`

Job status APIs:

- `GET /jobs` list job history
- `GET /jobs/:id` get one job
- `GET /jobs/:id/deliveries` get delivery-attempt history for a job

Supported job filters:

- `pipelineId`
- `status`
- `createdAfter`
- `createdBefore`
- `page`
- `pageSize`

Supported delivery filters:

- `status`
- `page`
- `pageSize`

Response format:

- list endpoints return `{ data, meta }`
- single-resource endpoints return `{ data }`
- errors return `{ error: { code, message, details? } }`

## Action Types

`TRANSFORM` copies values from the incoming payload into a new JSON object.

Input:

```json
{
  "event": { "id": "evt-123" },
  "customer": { "email": "user@example.com" },
  "total": 150
}
```

Config:

```json
{
  "fields": {
    "eventId": "event.id",
    "customerEmail": "customer.email",
    "total": "total"
  }
}
```

Output:

```json
{
  "eventId": "evt-123",
  "customerEmail": "user@example.com",
  "total": 150
}
```

`FILTER` evaluates conditions and can stop a job from being delivered.

Input:

```json
{
  "total": 150,
  "customer": { "email": "user@example.com" }
}
```

Config:

```json
{
  "match": "all",
  "conditions": [
    {
      "path": "total",
      "operator": "gt",
      "value": 100
    }
  ]
}
```

Output:

- If the condition matches, the payload continues through the pipeline.
- If it does not match, the job is marked `FILTERED_OUT` and subscriber delivery is skipped.
- Example: `total = 150` with rule `total > 100` continues.
- Example: `total = 50` with rule `total > 100` becomes `FILTERED_OUT`.

`ENRICH` adds static fields to the payload.

Input:

```json
{
  "eventId": "evt-123",
  "total": 150
}
```

Config:

```json
{
  "add": {
    "workflow": "orders",
    "source": "web-form"
  }
}
```

Output:

```json
{
  "eventId": "evt-123",
  "total": 150,
  "workflow": "orders",
  "source": "web-form"
}
```

Supported filter operators:

- `exists`
- `equals`
- `notEquals`
- `gt`
- `gte`
- `lt`
- `lte`

## Running Locally

Full stack with Docker Compose:

```bash
docker compose up --build
```

Services:

- `db`: PostgreSQL on `localhost:5433`
- `api`: HTTP server on `localhost:3000`
- `worker`: background job processor

Stop the stack:

```bash
docker compose down
```

Reset the database volume:

```bash
docker compose down -v
```

Local development without containerizing the app:

Install dependencies:

```bash
npm install
```

Run the API and worker locally against a PostgreSQL container:

```bash
docker compose up -d db
npm run prisma:deploy
npm run dev
```

API base URL:

```text
http://localhost:3000
```

## Example Requests

Example pipeline request:

```json
{
  "name": "Order Events Pipeline",
  "sourcePath": "/webhooks/order-events",
  "actionType": "TRANSFORM",
  "actionConfig": {
    "fields": {
      "eventId": "event.id",
      "customerEmail": "customer.email",
      "total": "total"
    }
  },
  "subscribers": [
    {
      "url": "https://example.com/hooks/orders",
      "active": true
    }
  ]
}
```

Example webhook payload:

```json
{
  "event": {
    "id": "evt-123"
  },
  "customer": {
    "email": "user@example.com"
  },
  "total": 150,
  "source": "web-form"
}
```

Example history query:

```text
GET /jobs?status=COMPLETED&page=1&pageSize=20
```

## Demo Flow

1. Create a pipeline with `POST /pipelines`
2. Send a webhook to the pipeline `sourcePath`
3. Query `GET /jobs` to see the queued or completed job
4. Query `GET /jobs/:id` for full job details
5. Query `GET /jobs/:id/deliveries` for downstream delivery history

## Testing

```bash
npm run lint
npm run build
npm test
```

## CI

GitHub Actions runs two parallel checks on pushes and pull requests to `main`:

- Node validation: install, Prisma generate, lint, build, test
- Docker validation: `docker compose up --build`, health check, and teardown
