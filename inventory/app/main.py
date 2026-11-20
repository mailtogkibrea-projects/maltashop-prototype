import uuid
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from .common import correlation_id, get_db, init_db, already_processed, publish, start_consumer, log

SCHEMA = """
CREATE TABLE IF NOT EXISTS stock (sku TEXT PRIMARY KEY, available INTEGER NOT NULL CHECK (available >= 0));
CREATE TABLE IF NOT EXISTS reservations (order_id TEXT PRIMARY KEY, sku TEXT, quantity INTEGER, status TEXT);
INSERT OR IGNORE INTO stock VALUES ('LAPTOP-1', 5), ('KETTLE-1', 10);
"""


def handle_event(rk, p, message_id):
    conn = get_db()
    out = None
    try:
        with conn:
            if already_processed(conn, message_id):
                log.info("duplicate message ignored")
                return
            if rk == "order.placed":
                # atomic reserve: the WHERE clause prevents overselling
                cur = conn.execute("UPDATE stock SET available = available - ? WHERE sku=? AND available >= ?",
                                   (p["quantity"], p["sku"], p["quantity"]))
                if cur.rowcount == 1:
                    conn.execute("INSERT INTO reservations VALUES (?,?,?,'RESERVED')", (p["order_id"], p["sku"], p["quantity"]))
                    out = ("stock.reserved", p)
                else:
                    out = ("stock.failed", {**p, "reason": "out_of_stock"})
            elif rk == "payment.failed":
                # COMPENSATING ACTION: release the stock that was reserved for this order
                r = conn.execute("SELECT sku, quantity FROM reservations WHERE order_id=? AND status='RESERVED'",
                                 (p["order_id"],)).fetchone()
                if r:
                    conn.execute("UPDATE stock SET available = available + ? WHERE sku=?", (r["quantity"], r["sku"]))
                    conn.execute("UPDATE reservations SET status='RELEASED' WHERE order_id=?", (p["order_id"],))
                    log.info(f"stock released for order {p['order_id']} (compensation)")
    finally:
        conn.close()
    if out:
        publish(out[0], out[1], message_id=f"{out[0]}:{p['order_id']}")


@asynccontextmanager
async def lifespan(app):
    init_db(SCHEMA)
    start_consumer("inventory.q", ["order.placed", "payment.failed"], handle_event)
    yield

app = FastAPI(title="Inventory service", lifespan=lifespan)


@app.middleware("http")
async def correlation_middleware(request: Request, call_next):
    correlation_id.set(request.headers.get("X-Correlation-ID") or str(uuid.uuid4()))
    return await call_next(request)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/stock")
def stock():
    conn = get_db()
    rows = [dict(r) for r in conn.execute("SELECT * FROM stock")]
    conn.close()
    return rows
