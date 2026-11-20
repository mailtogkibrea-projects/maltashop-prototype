import uuid
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from .common import correlation_id, get_db, init_db, already_processed, start_consumer, log

SCHEMA = """CREATE TABLE IF NOT EXISTS notifications (
  id INTEGER PRIMARY KEY AUTOINCREMENT, order_id TEXT, email TEXT, subject TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP);"""


def handle_event(rk, p, message_id):
    subject = "Your MaltaShop order is confirmed" if rk == "order.confirmed" else "Your MaltaShop order was cancelled"
    conn = get_db()
    try:
        with conn:
            if already_processed(conn, message_id):
                log.info("duplicate message ignored")
                return
            conn.execute("INSERT INTO notifications(order_id, email, subject) VALUES (?,?,?)",
                         (p["order_id"], p["customer_email"], subject))
    finally:
        conn.close()
    log.info(f"EMAIL (simulated) to {p['customer_email']}: {subject}")


@asynccontextmanager
async def lifespan(app):
    init_db(SCHEMA)
    start_consumer("notification.q", ["order.confirmed", "order.cancelled"], handle_event)
    yield

app = FastAPI(title="Notification service", lifespan=lifespan)


@app.middleware("http")
async def correlation_middleware(request: Request, call_next):
    correlation_id.set(request.headers.get("X-Correlation-ID") or str(uuid.uuid4()))
    return await call_next(request)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/notifications")
def notifications():
    conn = get_db()
    rows = [dict(r) for r in conn.execute("SELECT * FROM notifications ORDER BY id")]
    conn.close()
    return rows
