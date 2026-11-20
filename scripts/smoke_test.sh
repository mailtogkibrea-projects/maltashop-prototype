#!/usr/bin/env bash
# End-to-end check of the place-order saga. Needs: curl, jq. Run after `docker compose up -d --build --wait`.
set -euo pipefail
ORDER=${ORDER_URL:-http://localhost:8001}
INV=${INVENTORY_URL:-http://localhost:8002}
NOTI=${NOTIFICATION_URL:-http://localhost:8004}

place() {  # $1=correlation id  $2=sku  $3=qty  $4=amount
  curl -sf -X POST "$ORDER/orders" -H 'Content-Type: application/json' \
    -H "X-Correlation-ID: $1" -H "Idempotency-Key: $1" \
    -d "{\"customer_email\":\"student@maltashop.mt\",\"sku\":\"$2\",\"quantity\":$3,\"amount\":$4}"
}
wait_final() {
  for _ in $(seq 1 30); do
    s=$(curl -sf "$ORDER/orders/$1" | jq -r .status)
    [ "$s" != "PENDING" ] && { echo "$s"; return; }
    sleep 1
  done
  echo PENDING
}

echo "== 1. Happy path (expect CONFIRMED)"
CID="happy-$(date +%s)"
R=$(place "$CID" LAPTOP-1 1 899); echo "$R" | jq .
ID=$(echo "$R" | jq -r .id)
S=$(wait_final "$ID"); echo "final status: $S"; [ "$S" = CONFIRMED ]

echo "== 2. Idempotency: same key again returns the SAME order"
ID2=$(place "$CID" LAPTOP-1 1 899 | jq -r .id); echo "$ID == $ID2"; [ "$ID" = "$ID2" ]

echo "== 3. Payment declined -> compensation (expect CANCELLED, stock restored)"
BEFORE=$(curl -sf "$INV/stock" | jq '.[]|select(.sku=="KETTLE-1").available')
CID="declined-$(date +%s)"
ID=$(place "$CID" KETTLE-1 2 1500 | jq -r .id)
S=$(wait_final "$ID"); sleep 2
AFTER=$(curl -sf "$INV/stock" | jq '.[]|select(.sku=="KETTLE-1").available')
echo "status: $S | kettle stock before=$BEFORE after=$AFTER"; [ "$S" = CANCELLED ] && [ "$BEFORE" = "$AFTER" ]

echo "== 4. Out of stock (expect CANCELLED)"
ID=$(place "oos-$(date +%s)" LAPTOP-1 999 100 | jq -r .id)
S=$(wait_final "$ID"); echo "status: $S"; [ "$S" = CANCELLED ]

echo "== 5. Notification service received the events"
curl -sf "$NOTI/notifications" | jq .
echo "ALL CHECKS PASSED"
