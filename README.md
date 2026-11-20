# MaltaShop – "Place an order" prototype

A small event-driven microservices system that demonstrates the place-order flow with a **choreographed saga**
(no distributed transaction), idempotent consumers, correlation IDs and one-command startup.

## Services
| Service | Port | Owns (own SQLite file/volume) | Role |
|---|---|---|---|
| order | 8001 | orders, idempotency keys | REST `POST /orders`, saga participant, publishes `order.placed` |
| inventory | 8002 | stock, reservations | reserves stock; releases it on `payment.failed` (compensation) |
| payment | – | payments | simulated card provider (declines amounts above `DECLINE_ABOVE`) |
| notification | 8004 | notifications | consumes `order.confirmed` / `order.cancelled`, "sends" email |
| rabbitmq | 5672 / 15672 | – | message broker (management UI: http://localhost:15672, guest/guest) |

## Saga (choreography)
```
POST /orders -> Order(PENDING) --order.placed--> Inventory --stock.reserved--> Payment
   Payment --payment.succeeded--> Order(CONFIRMED) --order.confirmed--> Notification
   Payment --payment.failed--> Order(CANCELLED) + Inventory releases stock (compensation) --> Notification
   Inventory --stock.failed--> Order(CANCELLED) --order.cancelled--> Notification
```

## Run
Requires Docker with Compose (or use GitHub Codespaces – see below).
```bash
docker compose up -d --build --wait
./scripts/smoke_test.sh              # automated end-to-end check
docker compose logs -f               # JSON logs; filter by one correlation ID
docker compose down -v               # stop and delete data
```
Manual test:
```bash
curl -i -X POST localhost:8001/orders -H 'Content-Type: application/json' \
  -H 'X-Correlation-ID: demo-123' -H 'Idempotency-Key: demo-123' \
  -d '{"customer_email":"a@b.mt","sku":"LAPTOP-1","quantity":1,"amount":899}'
docker compose logs | grep demo-123     # same correlation ID in all services
```
Seed data: `LAPTOP-1` (stock 5), `KETTLE-1` (stock 10). Amount > 1000 is declined by the fake payment provider.

## Environment variables
| Variable | Used by | Default | Meaning |
|---|---|---|---|
| `SERVICE_NAME` | all | set in compose | appears in every log line |
| `RABBITMQ_URL` | all | built from user/pass | broker connection |
| `RABBITMQ_USER` / `RABBITMQ_PASS` | compose | guest / guest | broker credentials |
| `DB_PATH` | all | `/data/<service>.db` | location of the service's own database |
| `DECLINE_ABOVE` | payment | 1000 | amount above which payment is declined |

## Known limits (see report 6.3)
Fake payment provider; SQLite instead of PostgreSQL; no transactional outbox; no dead-letter queue; no authentication.
