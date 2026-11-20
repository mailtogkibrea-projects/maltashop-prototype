import os, sqlite3, uuid
from contextlib import asynccontextmanager
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field
from .common import correlation_id, get_db, init_db, already_processed, publish, start_consumer, log

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
  id TEXT PRIMARY KEY, customer_email TEXT, sku TEXT, quantity INTEGER, amount REAL,
  status TEXT, reason TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS idempotency_keys (key TEXT PRIMARY KEY, order_id TEXT);
"""


def handle_event(rk, p, message_id):
    """SAGA (choreography): Order reacts to the outcomes of Inventory and Payment."""
    conn = get_db()
    out = None
    try:
        with conn:
            if already_processed(conn, message_id):          # idempotent consumer
                log.info("duplicate message ignored")
                return
            if rk == "stock.failed":
                conn.execute("UPDATE orders SET status='CANCELLED', reason=? WHERE id=?", (p.get("reason"), p["order_id"]))
                out = "order.cancelled"
            elif rk == "payment.failed":
                conn.execute("UPDATE orders SET status='CANCELLED', reason='payment_declined' WHERE id=?", (p["order_id"],))
                out = "order.cancelled"
            elif rk == "payment.succeeded":
                conn.execute("UPDATE orders SET status='CONFIRMED' WHERE id=?", (p["order_id"],))
                out = "order.confirmed"
    finally:
        conn.close()
    if out:
        publish(out, p, message_id=f"{out}:{p['order_id']}")


@asynccontextmanager
async def lifespan(app):
    init_db(SCHEMA)
    start_consumer("order.q", ["stock.failed", "payment.succeeded", "payment.failed"], handle_event)
    yield

app = FastAPI(title="Order service", lifespan=lifespan)


@app.middleware("http")
async def correlation_middleware(request: Request, call_next):
    cid = request.headers.get("X-Correlation-ID") or str(uuid.uuid4())
    correlation_id.set(cid)
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = cid
    return response


class OrderIn(BaseModel):
    customer_email: str
    sku: str
    quantity: int = Field(gt=0)
    amount: float = Field(gt=0)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/orders", status_code=202)
def place_order(body: OrderIn, idempotency_key: str | None = Header(default=None)):
    order_id = str(uuid.uuid4())
    conn = get_db()
    try:
        with conn:
            if idempotency_key:                                # idempotent REST endpoint
                row = conn.execute("SELECT order_id FROM idempotency_keys WHERE key=?", (idempotency_key,)).fetchone()
                if row:
                    log.info("replayed request, returning existing order")
                    return get_order(row["order_id"])
                conn.execute("INSERT INTO idempotency_keys(key, order_id) VALUES (?,?)", (idempotency_key, order_id))
            conn.execute("INSERT INTO orders(id, customer_email, sku, quantity, amount, status) VALUES (?,?,?,?,?,'PENDING')",
                         (order_id, body.customer_email, body.sku, body.quantity, body.amount))
    finally:
        conn.close()
    log.info(f"order {order_id} created PENDING")
    publish("order.placed", {"order_id": order_id, **body.model_dump()}, message_id=f"order.placed:{order_id}")
    return get_order(order_id)


@app.get("/orders/{order_id}")
def get_order(order_id: str):
    conn = get_db()
    row = conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "order not found")
    return dict(row)
