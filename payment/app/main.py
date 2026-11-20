import uuid
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from .common import correlation_id, get_db, init_db, already_processed, publish, start_consumer, log

SCHEMA = "CREATE TABLE IF NOT EXISTS payments (order_id TEXT PRIMARY KEY, amount REAL, status TEXT);"
DECLINE_ABOVE = float(__import__("os").getenv("DECLINE_ABOVE", "1000"))   # fake provider rule for the demo


def handle_event(rk, p, message_id):
    conn = get_db()
    try:
        with conn:
            if already_processed(conn, message_id):
                log.info("duplicate message ignored")
                return
            ok = p["amount"] <= DECLINE_ABOVE                     # stand-in for the external card provider
            conn.execute("INSERT OR REPLACE INTO payments VALUES (?,?,?)",
                         (p["order_id"], p["amount"], "SUCCEEDED" if ok else "FAILED"))
    finally:
        conn.close()
    event = "payment.succeeded" if ok else "payment.failed"
    publish(event, p, message_id=f"{event}:{p['order_id']}")


@asynccontextmanager
async def lifespan(app):
    init_db(SCHEMA)
    start_consumer("payment.q", ["stock.reserved"], handle_event)
    yield

app = FastAPI(title="Payment service", lifespan=lifespan)


@app.middleware("http")
async def correlation_middleware(request: Request, call_next):
    correlation_id.set(request.headers.get("X-Correlation-ID") or str(uuid.uuid4()))
    return await call_next(request)


@app.get("/health")
def health():
    return {"status": "ok"}
