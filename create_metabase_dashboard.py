import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"


def load_env_file(file_path):
    if not file_path.exists():
        return

    for line in file_path.read_text().splitlines():
        line = line.strip()

        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_env_file(ENV_FILE)

METABASE_URL = os.getenv("METABASE_URL", "http://localhost:3000").rstrip("/")
METABASE_EMAIL = os.getenv("METABASE_EMAIL")
METABASE_PASSWORD = os.getenv("METABASE_PASSWORD")
METABASE_DB_NAME = os.getenv("METABASE_DB_NAME", "Retail Data Warehouse")


def request(method, path, token=None, data=None):
    body = None
    headers = {"Content-Type": "application/json"}

    if token:
        headers["X-Metabase-Session"] = token

    if data is not None:
        body = json.dumps(data).encode("utf-8")

    req = urllib.request.Request(
        f"{METABASE_URL}{path}",
        data=body,
        headers=headers,
        method=method,
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        message = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} failed: {exc.code} {message}") from exc


def login():
    if not METABASE_EMAIL or not METABASE_PASSWORD:
        raise RuntimeError(
            "Set METABASE_EMAIL and METABASE_PASSWORD before running this script."
        )

    response = request(
        "POST",
        "/api/session",
        data={"username": METABASE_EMAIL, "password": METABASE_PASSWORD},
    )
    return response["id"]


def get_or_create_database(token):
    databases = request("GET", "/api/database", token=token)["data"]

    for database in databases:
        if database["name"] == METABASE_DB_NAME:
            return database["id"]

    payload = {
        "engine": "postgres",
        "name": METABASE_DB_NAME,
        "details": {
            "host": os.getenv("METABASE_POSTGRES_HOST", "retail-postgres"),
            "port": int(os.getenv("METABASE_POSTGRES_PORT", "5432")),
            "dbname": os.getenv("POSTGRES_DB", "retail_dw"),
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
            "ssl": False,
            "tunnel-enabled": False,
        },
        "auto_run_queries": True,
        "is_full_sync": True,
        "schedules": {},
    }

    database = request("POST", "/api/database", token=token, data=payload)
    return database["id"]


def create_dashboard(token):
    return request(
        "POST",
        "/api/dashboard",
        token=token,
        data={
            "name": "project data wareheouse dashboard",
            "description": (
                "Executive retail data warehouse dashboard built from the dbt gold layer: "
                "business health, sales trends, category performance, customer behavior, "
                "and promotion ROI."
            ),
        },
    )["id"]


def create_card(token, database_id, name, query, display, settings=None):
    payload = {
        "name": name,
        "dataset_query": {
            "database": database_id,
            "type": "native",
            "native": {"query": query, "template-tags": {}},
        },
        "display": display,
        "visualization_settings": settings or {},
    }
    return request("POST", "/api/card", token=token, data=payload)["id"]


def add_cards(token, dashboard_id, dashcards):
    request(
        "PUT",
        f"/api/dashboard/{dashboard_id}/cards",
        token=token,
        data={"cards": dashcards},
    )


def main():
    token = login()
    database_id = get_or_create_database(token)
    dashboard_id = create_dashboard(token)

    currency = {
        "column_settings": {
            '["name","total_revenue"]': {"number_style": "currency"},
            '["name","promo_revenue"]': {"number_style": "currency"},
            '["name","target_revenue"]': {"number_style": "currency"},
            '["name","revenue_gap"]': {"number_style": "currency"},
            '["name","avg_order_value"]': {"number_style": "currency"},
            '["name","avg_customer_revenue"]': {"number_style": "currency"},
            '["name","promo_attributed_revenue"]': {"number_style": "currency"},
        }
    }

    cards = [
        {
            "name": "Total paid revenue",
            "display": "scalar",
            "query": """
                select
                    sum(total_revenue) as total_revenue
                from gold.mart_daily_sales
            """,
            "settings": currency,
            "layout": (0, 0, 6, 3),
        },
        {
            "name": "Promo-attributed revenue",
            "display": "scalar",
            "query": """
                select
                    sum(promo_attributed_revenue) as promo_attributed_revenue
                from gold.mart_daily_sales
            """,
            "settings": currency,
            "layout": (0, 6, 6, 3),
        },
        {
            "name": "Total paid orders",
            "display": "scalar",
            "query": """
                select
                    sum(total_orders) as total_orders
                from gold.mart_daily_sales
            """,
            "layout": (0, 12, 6, 3),
        },
        {
            "name": "Total customers",
            "display": "scalar",
            "query": """
                select
                    count(*) as unique_customers
                from gold.mart_customer_summary
            """,
            "layout": (0, 18, 6, 3),
        },
        {
            "name": "Revenue goal this quarter",
            "display": "progress",
            "query": """
                select
                    sum(total_revenue) as total_revenue,
                    250000::numeric as target_revenue
                from gold.mart_daily_sales
                where order_day >= date_trunc('quarter', current_date)
                  and order_day < date_trunc('quarter', current_date) + interval '3 months'
            """,
            "settings": currency,
            "layout": (3, 0, 6, 6),
        },
        {
            "name": "Revenue and orders over time",
            "display": "combo",
            "query": """
                select
                    date_trunc('month', order_day)::date as order_month,
                    sum(total_revenue) as total_revenue,
                    sum(total_orders) as total_orders
                from gold.mart_daily_sales
                group by order_month
                order by order_month
            """,
            "settings": currency,
            "layout": (3, 6, 18, 6),
        },
        {
            "name": "Revenue trend by day",
            "display": "line",
            "query": """
                select
                    order_day,
                    sum(total_revenue) as total_revenue,
                    sum(promo_attributed_revenue) as promo_attributed_revenue
                from gold.mart_daily_sales
                group by order_day
                order by order_day
            """,
            "settings": currency,
            "layout": (9, 0, 15, 7),
        },
        {
            "name": "Revenue by order channel",
            "display": "bar",
            "query": """
                select
                    order_channel,
                    sum(total_revenue) as total_revenue
                from gold.mart_daily_sales
                group by order_channel
                order by total_revenue desc
            """,
            "settings": currency,
            "layout": (9, 15, 9, 7),
        },
        {
            "name": "Product category performance",
            "display": "bar",
            "query": """
                select
                    product_category,
                    total_revenue,
                    total_quantity_sold
                from gold.mart_product_category_performance
                order by total_revenue desc
            """,
            "settings": currency,
            "layout": (16, 0, 12, 7),
        },
        {
            "name": "Revenue by income level",
            "display": "bar",
            "query": """
                select
                    income_level,
                    sum(total_revenue) as total_revenue,
                    avg(total_revenue) as avg_customer_revenue
                from gold.mart_customer_summary
                group by income_level
                order by total_revenue desc
            """,
            "settings": currency,
            "layout": (16, 12, 12, 7),
        },
        {
            "name": "Customer segment value",
            "display": "bar",
            "query": """
                select
                    customer_segment,
                    count(*) as customers,
                    sum(total_revenue) as total_revenue
                from gold.mart_customer_summary
                group by customer_segment
                order by total_revenue desc
            """,
            "settings": currency,
            "layout": (23, 0, 12, 7),
        },
        {
            "name": "Promotion ROI leaderboard",
            "display": "table",
            "query": """
                select
                    campaign_name,
                    promo_type,
                    campaign_channel,
                    target_segment,
                    total_revenue,
                    paid_orders,
                    unique_customers,
                    revenue_roi
                from gold.mart_promotion_performance
                where paid_orders > 0
                order by revenue_roi desc nulls last
                limit 15
            """,
            "settings": currency,
            "layout": (23, 12, 12, 7),
        },
        {
            "name": "Promo sensitivity distribution",
            "display": "bar",
            "query": """
                with sensitivity_summary as (
                    select
                        case
                            when promo_sensitivity < 0.4 then 'Low'
                            when promo_sensitivity < 0.7 then 'Medium'
                            else 'High'
                        end as sensitivity_group,
                        count(*) as customers,
                        sum(total_revenue) as total_revenue
                    from gold.mart_customer_summary
                    where promo_sensitivity is not null
                    group by 1
                )
                select
                    sensitivity_group,
                    customers,
                    total_revenue
                from sensitivity_summary
                order by
                    case sensitivity_group
                        when 'Low' then 1
                        when 'Medium' then 2
                        else 3
                    end
            """,
            "settings": currency,
            "layout": (30, 0, 12, 6),
        },
        {
            "name": "Top customers by revenue",
            "display": "table",
            "query": """
                select
                    customer_id,
                    first_name,
                    last_name,
                    customer_segment,
                    income_level,
                    paid_orders,
                    total_revenue,
                    promo_revenue,
                    last_order_date
                from gold.mart_customer_summary
                order by total_revenue desc
                limit 20
            """,
            "settings": currency,
            "layout": (30, 12, 12, 6),
        },
    ]

    dashcards = []

    for index, card in enumerate(cards, start=1):
        card_id = create_card(
            token,
            database_id,
            card["name"],
            card["query"],
            card["display"],
            card.get("settings"),
        )
        row, col, size_x, size_y = card["layout"]
        dashcards.append(
            {
                "id": -index,
                "card_id": card_id,
                "row": row,
                "col": col,
                "size_x": size_x,
                "size_y": size_y,
                "parameter_mappings": [],
                "visualization_settings": {},
            }
        )

    add_cards(token, dashboard_id, dashcards)

    print(f"Created dashboard: {METABASE_URL}/dashboard/{dashboard_id}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Dashboard creation failed: {exc}", file=sys.stderr)
        sys.exit(1)
