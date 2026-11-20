"""Small per-service helper: JSON logging + correlation ID, RabbitMQ publish/consume, idempotency.
Each service carries its own copy so services share no code at runtime (loose coupling)."""
import contextvars, json, logging, os, sqlite3, threading, time, uuid
import pika

SERVICE = os.getenv("SERVICE_NAME", "service")
RABBIT_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/%2F")
DB_PATH = os.getenv("DB_PATH", f"/data/{SERVICE}.db")
EXCHANGE = "maltashop.events"

correlation_id = contextvars.ContextVar("correlation_id", default="-")


class JsonFormatter(logging.Formatter):
    """Mandatory structured log schema: ts, level, service, correlation_id, message."""
    def format(self, r):
        return json.dumps({
            "ts": self.formatTime(r, "%Y-%m-%dT%H:%M:%S"),
            "level": r.levelname,
            "service": SERVICE,
            "correlation_id": correlation_id.get(),
            "message": r.getMessage() + (" | " + self.formatException(r.exc_info) if r.exc_info else ""),
        })


def setup_logging():
    h = logging.StreamHandler()
    h.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [h]
    root.setLevel(logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARNING)
    return logging.getLogger(SERVICE)


log = setup_logging()


# ---------- database (each service owns its own SQLite file) ----------
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(schema_sql):
    conn = get_db()
    with conn:
        conn.executescript(schema_sql + """
        CREATE TABLE IF NOT EXISTS processed_messages (message_id TEXT PRIMARY KEY);""")
    conn.close()


def already_processed(conn, message_id):
    """Idempotent consumer: the dedupe row is written in the SAME transaction as the business change."""
    try:
        conn.execute("INSERT INTO processed_messages(message_id) VALUES (?)", (message_id,))
        return False
    except sqlite3.IntegrityError:
        return True


# ---------- messaging ----------
def _connect():
    params = pika.URLParameters(RABBIT_URL)
    for _ in range(30):
        try:
            return pika.BlockingConnection(params)
        except pika.exceptions.AMQPConnectionError:
            time.sleep(2)
    raise RuntimeError("RabbitMQ not reachable")


def publish(routing_key, payload, message_id=None):
    """Publish a durable message with publisher confirms. The correlation ID travels in the headers."""
    conn = _connect()
    try:
        ch = conn.channel()
        ch.exchange_declare(EXCHANGE, exchange_type="topic", durable=True)
        ch.confirm_delivery()
        props = pika.BasicProperties(
            delivery_mode=2, content_type="application/json",
            message_id=message_id or str(uuid.uuid4()),
            headers={"correlation_id": correlation_id.get()})
        ch.basic_publish(EXCHANGE, routing_key, json.dumps(payload).encode(), props)
        log.info(f"published {routing_key}")
    finally:
        conn.close()


def start_consumer(queue, bindings, handler):
    """Run a durable-queue consumer in a background thread; reconnects on failure."""
    def run():
        while True:
            try:
                conn = _connect()
                ch = conn.channel()
                ch.exchange_declare(EXCHANGE, exchange_type="topic", durable=True)
                ch.queue_declare(queue, durable=True)
                for b in bindings:
                    ch.queue_bind(queue, EXCHANGE, b)
                ch.basic_qos(prefetch_count=1)

                def on_message(ch_, method, props, body):
                    token = correlation_id.set((props.headers or {}).get("correlation_id", "-"))
                    try:
                        log.info(f"received {method.routing_key}")
                        handler(method.routing_key, json.loads(body), props.message_id)
                        ch_.basic_ack(method.delivery_tag)
                    except Exception:
                        log.exception("handler failed; message rejected")
                        ch_.basic_nack(method.delivery_tag, requeue=False)  # would go to a dead-letter queue in production
                    finally:
                        correlation_id.reset(token)

                ch.basic_consume(queue, on_message)
                ch.start_consuming()
            except Exception:
                log.exception("consumer connection lost; retrying")
                time.sleep(3)
    threading.Thread(target=run, daemon=True).start()
