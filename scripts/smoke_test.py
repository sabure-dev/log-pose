#!/usr/bin/env python3

import sys
import time
import uuid
import urllib.request
import urllib.error
import json

GATEWAY_URL = "http://localhost:8080"


def request(
    method: str, path: str, payload: dict | None = None, token: str | None = None
) -> tuple[int, dict]:
    url = f"{GATEWAY_URL}{path}"
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as err:
        body = err.read().decode("utf-8")
        try:
            parsed = json.loads(body)
        except Exception:
            parsed = {"raw": body}
        return err.code, parsed


def main():
    print(f"=== Starting E2E Smoke Test against {GATEWAY_URL} ===")

    print("[1/8] Checking Gateway Health...")
    status, data = request("GET", "/health")
    if status != 200:
        print(f"FAILED: Gateway not healthy (status {status}): {data}")
        sys.exit(1)
    print("      Gateway is healthy!")

    customer_username = f"customer_{uuid.uuid4().hex[:8]}"
    customer_password = "password123"
    print(f"[2/8] Registering Customer ({customer_username})...")
    status, cust_data = request(
        "POST",
        "/api/v1/auth/register",
        {
            "username": customer_username,
            "password": customer_password,
        },
    )
    if status != 201:
        print(f"FAILED: Customer registration returned {status}: {cust_data}")
        sys.exit(1)
    customer_token = cust_data["token"]
    customer_id = cust_data["user_id"]
    print(f"      Customer registered! ID={customer_id}")

    print("      Testing Privilege Escalation Prevention...")
    rogue_status, rogue_data = request(
        "POST",
        "/api/v1/auth/register",
        {
            "username": f"rogue_{uuid.uuid4().hex[:8]}",
            "password": "password123",
            "role": "warehouse_operator",
        },
    )
    if rogue_status != 403:
        print(
            f"FAILED: Expected 403 Forbidden for privileged self-registration, got {rogue_status}: {rogue_data}"
        )
        sys.exit(1)
    print("      Privilege escalation attempt properly rejected with 403 Forbidden!")

    operator_username = f"operator_{uuid.uuid4().hex[:8]}"
    operator_password = "operatorpass123"
    print(f"[3/8] Provisioning Warehouse Operator ({operator_username}) via Admin...")

    admin_status, admin_data = request(
        "POST",
        "/api/v1/auth/login",
        {
            "username": "admin",
            "password": "adminpass123",
        },
    )
    if admin_status != 200:
        print(f"FAILED: Admin login returned {admin_status}: {admin_data}")
        sys.exit(1)
    admin_token = admin_data["token"]

    prov_status, prov_data = request(
        "POST",
        "/api/v1/admin/users",
        {
            "username": operator_username,
            "password": operator_password,
            "role": "warehouse_operator",
        },
        token=admin_token,
    )
    if prov_status != 201:
        print(
            f"FAILED: Admin operator provisioning returned {prov_status}: {prov_data}"
        )
        sys.exit(1)

    oper_login_status, oper_login_data = request(
        "POST",
        "/api/v1/auth/login",
        {
            "username": operator_username,
            "password": operator_password,
        },
    )
    if oper_login_status != 200:
        print(f"FAILED: Operator login returned {oper_login_status}: {oper_login_data}")
        sys.exit(1)
    operator_token = oper_login_data["token"]
    print(f"      Operator {operator_username} provisioned and authenticated!")

    print("[4/8] Browsing warehouse catalog via Gateway...")
    status, catalog = request("GET", "/api/v1/inventory/products")
    if status != 200 or not catalog.get("products"):
        print(f"FAILED: Catalog fetch returned {status}: {catalog}")
        sys.exit(1)
    products = catalog["products"]
    print(f"      Found {len(products)} products in catalog:")
    target_product = products[0]
    print(
        f"      Target item: {target_product['name']} (ID={target_product['product_id']}, Price={target_product['price']}, Available={target_product['available']})"
    )

    print("[5/8] Checking stock availability via Gateway...")
    status, check_res = request(
        "POST",
        "/api/v1/inventory/check",
        {
            "items": [
                {
                    "product_id": target_product["product_id"],
                    "quantity": 1,
                    "expected_price": target_product["price"],
                }
            ]
        },
    )
    if status != 200 or not check_res.get("eligible"):
        print(f"FAILED: Pre-flight check failed: {check_res}")
        sys.exit(1)
    print("      Pre-flight availability confirmed eligible!")

    print("[6/8] Creating order as Customer...")
    status, order = request(
        "POST",
        "/api/v1/orders/",
        {
            "items": [
                {
                    "product_id": target_product["product_id"],
                    "quantity": 1,
                    "price": target_product["price"],
                }
            ],
            "shipping_address": "42 Cloud Avenue, Tokyo",
        },
        token=customer_token,
    )

    if status != 201:
        print(f"FAILED: Order creation returned {status}: {order}")
        sys.exit(1)
    order_id = order["id"]
    print(f"      Order created! ID={order_id}, initial status={order['status']}")

    print("      Waiting for Choreographed Saga to transition order to 'reserved'...")
    reserved = False
    for attempt in range(1, 15):
        time.sleep(1)
        status, poll_order = request(
            "GET", f"/api/v1/orders/{order_id}", token=customer_token
        )
        if status == 200 and poll_order.get("status") == "reserved":
            reserved = True
            print(
                f"      Saga success! Order transitioned to 'reserved' in attempt {attempt}."
            )
            break
        elif status == 200 and poll_order.get("status") == "cancelled":
            print(
                f"FAILED: Order was cancelled by saga: {poll_order.get('cancellation_reason')}"
            )
            sys.exit(1)

    if not reserved:
        print("FAILED: Timeout waiting for order to become reserved.")
        sys.exit(1)

    print(f"[7/8] Operator transitioning order {order_id} to 'ready_for_shipping'...")
    status, ship_res = request(
        "POST", f"/api/v1/orders/{order_id}/ship", token=operator_token
    )
    if status != 200:
        print(f"FAILED: Ship order failed {status}: {ship_res}")
        sys.exit(1)
    print(f"      Order status: {ship_res['status']}")

    print(f"[8/8] Operator completing order {order_id}...")
    status, comp_res = request(
        "POST", f"/api/v1/orders/{order_id}/complete", token=operator_token
    )
    if status != 200:
        print(f"FAILED: Complete order failed {status}: {comp_res}")
        sys.exit(1)
    print(f"      Order status: {comp_res['status']}")

    status, final_order = request(
        "GET", f"/api/v1/orders/{order_id}", token=customer_token
    )
    if status == 200 and final_order.get("status") == "completed":
        print("\n========================================================")
        print("  ALL E2E SMOKE TESTS PASSED CLEANLY THROUGH GATEWAY!   ")
        print("========================================================")
    else:
        print(f"FAILED: Final status check failed: {final_order}")
        sys.exit(1)


if __name__ == "__main__":
    main()
