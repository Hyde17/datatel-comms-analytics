import os
from google.cloud import bigquery
import pandas as pd
from core.db import engine


PROJECT_ID = os.getenv("PROJECT_ID")
DATASET = os.getenv("DATASET") or "datatel_warehouse"
client = bigquery.Client(project=PROJECT_ID)


def load_postgres_table_to_bigquery(postgres_table: str, bigquery_table: str):
    """Load a PostgreSQL table to BigQuery."""
    print(
        f"Loading {postgres_table} from PostgreSQL to BigQuery as {bigquery_table}..."
    )
    query = f"SELECT * FROM {postgres_table}"
    df = pd.read_sql(query, engine)

    table_id = f"{PROJECT_ID}.{DATASET}.{bigquery_table}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows to {bigquery_table}")


def load_source_data():
    """Load all source tables from PostgreSQL to BigQuery."""
    print("Loading source data from PostgreSQL to BigQuery...\n")
    load_postgres_table_to_bigquery("src_customers", "stg_customers")
    load_postgres_table_to_bigquery("src_billing_transactions", "stg_billing")
    load_postgres_table_to_bigquery("src_network_sessions", "stg_sessions")
    print("Source data loaded successfully\n")


STG_BILLING_QUERY = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.stg_billing_deduplicated` AS

WITH ranked_billing AS (

    SELECT
        transaction_id,

        customer_id,

        COALESCE(amount, 0) AS amount,

        TIMESTAMP(transaction_date) AS transaction_date,

        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY TIMESTAMP(transaction_date) DESC
        ) AS rn

    FROM `{PROJECT_ID}.{DATASET}.stg_billing`
)

SELECT
    transaction_id,
    customer_id,
    amount,
    transaction_date

FROM ranked_billing

WHERE rn = 1;
"""

STG_SESSIONS_QUERY = f"""CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.stg_sessions` AS
SELECT session_id,
customer_id,
COALESCE(data_used_mb, 0) AS data_used_mb,
TIMESTAMP(start_time) AS start_time,
TIMESTAMP(end_time) AS end_time,
CASE
WHEN TIMESTAMP(end_time) > TIMESTAMP(start_time)
THEN TIMESTAMP_DIFF(
TIMESTAMP(end_time),
TIMESTAMP(start_time),
SECOND
)
ELSE 0
END AS session_duration_sec
FROM `{PROJECT_ID}.{DATASET}.src_network_sessions` ; """


STG_CUSTOMERS_QUERY = f""" CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.stg_customers`
AS
SELECT customer_id,
INITCAP(name) AS name,
LOWER(email) AS email,
COALESCE(country, 'Nigeria') AS country,
TIMESTAMP(created_at) AS created_at
FROM `{PROJECT_ID}.{DATASET}.src_customers` ; """


AGG_USER_REVENUE_QUERY = f""" CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.agg_user_revenue` AS
SELECT customer_id,
SUM(amount) AS total_revenue,
COUNT(transaction_id) AS total_transactions
FROM `{PROJECT_ID}.{DATASET}.stg_billing`
WHERE customer_id IS NOT NULL
GROUP BY customer_id; """


AGG_USER_USAGE_QUERY = f"""CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.agg_user_usage` AS
SELECT customer_id,
SUM(data_used_mb) AS total_data_used_mb,
AVG(
CASE
WHEN TIMESTAMP(end_time) > TIMESTAMP(start_time)
THEN TIMESTAMP_DIFF(
TIMESTAMP(end_time),
TIMESTAMP(start_time),
SECOND
)
ELSE 0
END
) AS avg_session_duration_sec,
COUNT(session_id) AS total_sessions
FROM `{PROJECT_ID}.{DATASET}.stg_sessions`
WHERE customer_id IS NOT NULL
GROUP BY customer_id; """

AGG_MONTHLY_REVENUE_QUERY = f"""CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.agg_monthly_revenue` AS
SELECT customer_id,
DATE_TRUNC(
 DATE(TIMESTAMP(transaction_date)),
 MONTH
) AS month,
SUM(amount) AS total_revenue,
COUNT(transaction_id) AS total_transactions
FROM `{PROJECT_ID}.{DATASET}.stg_billing`
WHERE customer_id IS NOT NULL
GROUP BY customer_id, month; """


AGG_ARPU_QUERY = f""" CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.agg_arpu` AS
SELECT customer_id,
SUM(amount) AS total_revenue,
COUNT(
 DISTINCT DATE_TRUNC(
 DATE(TIMESTAMP(transaction_date)),
 MONTH
 )
) AS active_months,
SUM(amount) / NULLIF(
 COUNT(
 DISTINCT DATE_TRUNC(
 DATE(TIMESTAMP(transaction_date)),
 MONTH
 )
 ),
 0
) AS arpu
FROM `{PROJECT_ID}.{DATASET}.stg_billing`
WHERE customer_id IS NOT NULL
GROUP BY customer_id; """


SESSION_BUCKETS_QUERY = f"""CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.session_buckets` AS
SELECT session_id,
customer_id,
data_used_mb,
start_time,
end_time,
CASE
WHEN TIMESTAMP(end_time) <= TIMESTAMP(start_time)
THEN 0
ELSE TIMESTAMP_DIFF(
TIMESTAMP(end_time),
TIMESTAMP(start_time),
SECOND
)
END AS session_duration_sec,
CASE
WHEN
CASE
WHEN TIMESTAMP(end_time) <= TIMESTAMP(start_time)
THEN 0
ELSE TIMESTAMP_DIFF(
TIMESTAMP(end_time),
TIMESTAMP(start_time),
SECOND
)
END < 60
THEN 'short'
WHEN
CASE
WHEN TIMESTAMP(end_time) <= TIMESTAMP(start_time)
THEN 0
ELSE TIMESTAMP_DIFF(
TIMESTAMP(end_time),
TIMESTAMP(start_time),
SECOND
)
END BETWEEN 60 AND 299
THEN 'medium'
ELSE 'long'
END AS session_type
FROM `{PROJECT_ID}.{DATASET}.stg_sessions` ; """


AGG_SESSION_DISTRIBUTION_QUERY = f"""CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.agg_session_distribution` AS
SELECT customer_id,
COUNTIF(session_type = 'short') AS short_sessions,
COUNTIF(session_type = 'medium') AS medium_sessions,
COUNTIF(session_type = 'long') AS long_sessions
FROM `{PROJECT_ID}.{DATASET}.session_buckets`
WHERE customer_id IS NOT NULL
GROUP BY customer_id; """


DW_USER_ANALYTICS_QUERY = f""" CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.dw_user_analytics` AS
SELECT CAST(c.customer_id AS INT64) AS customer_id,
c.name AS customer_name,
c.email,
c.country,
COALESCE(r.total_revenue, 0) AS total_revenue,
COALESCE(r.total_transactions, 0) AS total_transactions,
COALESCE(u.total_data_used_mb, 0) AS total_data_used_mb,
COALESCE(u.avg_session_duration_sec, 0)
AS avg_session_duration_sec,
COALESCE(u.total_sessions, 0) AS total_sessions,
COALESCE(a.arpu, 0) AS arpu,
COALESCE(s.short_sessions, 0) AS short_sessions,
COALESCE(s.medium_sessions, 0) AS medium_sessions,
COALESCE(s.long_sessions, 0) AS long_sessions,
COALESCE(u.total_data_used_mb, 0)
/ NULLIF(COALESCE(u.total_sessions, 0), 0)
AS avg_data_per_session_mb,
CASE
WHEN COALESCE(r.total_revenue, 0) > 5000000
THEN 'High Value'
WHEN COALESCE(r.total_revenue, 0) > 1000000
THEN 'Mid Value'
ELSE 'Low Value'
END AS customer_segment
FROM `{PROJECT_ID}.{DATASET}.stg_customers` c
LEFT JOIN `{PROJECT_ID}.{DATASET}.agg_user_revenue` r ON CAST(c.customer_id AS INT64) =
CAST(r.customer_id AS INT64)
LEFT JOIN `{PROJECT_ID}.{DATASET}.agg_user_usage` u ON CAST(c.customer_id AS INT64) =
CAST(u.customer_id AS INT64)
LEFT JOIN `{PROJECT_ID}.{DATASET}.agg_arpu` a ON CAST(c.customer_id AS INT64) =
CAST(a.customer_id AS INT64)
LEFT JOIN `{PROJECT_ID}.{DATASET}.agg_session_distribution` s ON CAST(c.customer_id AS
INT64) = CAST(s.customer_id AS INT64); """


TOP_CUSTOMERS_QUERY = f""" SELECT customer_id, customer_name, email, country, total_revenue,
total_transactions
FROM `{PROJECT_ID}.{DATASET}.dw_user_analytics`
ORDER BY total_revenue DESC
LIMIT 10; """
CUSTOMER_SEGMENTATION_QUERY = f""" SELECT customer_id,
customer_name,
email,
country,
total_revenue,
total_transactions,
arpu,
CASE
WHEN total_revenue > 5000000
THEN 'High Value'
WHEN total_revenue > 1000000
THEN 'Mid Value'
ELSE 'Low Value'
END AS customer_segment
FROM `{PROJECT_ID}.{DATASET}.dw_user_analytics` ; """

CHURN_RISK_QUERY = f""" SELECT customer_id,
customer_name,
email,
country,
total_revenue,
total_sessions,
arpu,
CASE
WHEN total_sessions < 5
AND total_revenue < 1000
THEN 'High Risk'
ELSE 'Active'
END AS churn_risk
FROM `{PROJECT_ID}.{DATASET}.dw_user_analytics` ; """

REVENUE_USAGE_MISMATCH_QUERY = f""" SELECT customer_id,
customer_name,
email,
country,
total_data_used_mb,
total_revenue,
total_sessions,
arpu
FROM `{PROJECT_ID}.{DATASET}.dw_user_analytics`
WHERE total_data_used_mb > 10000 AND total_revenue < 500
ORDER BY total_data_used_mb DESC; """


def run_query(query: str, description: str):
    print(f"Running: {description}")
    job = client.query(query)
    job.result()
    print(f"Completed: {description}\n")


def create_stg_billing():
    run_query(STG_BILLING_QUERY, "stg_billing")


def create_stg_sessions():
    run_query(STG_SESSIONS_QUERY, "stg_sessions")


def create_stg_customers():
    run_query(STG_CUSTOMERS_QUERY, "stg_customers")


def create_agg_user_revenue():
    run_query(AGG_USER_REVENUE_QUERY, "agg_user_revenue")


def create_agg_user_usage():
    run_query(AGG_USER_USAGE_QUERY, "agg_user_usage")


def create_agg_monthly_revenue():
    run_query(AGG_MONTHLY_REVENUE_QUERY, "agg_monthly_revenue")


def create_agg_arpu():
    run_query(AGG_ARPU_QUERY, "agg_arpu")


def create_session_buckets():
    run_query(SESSION_BUCKETS_QUERY, "session_buckets")


def create_agg_session_distribution():
    run_query(AGG_SESSION_DISTRIBUTION_QUERY, "agg_session_distribution")


def create_dw_user_analytics():
    run_query(DW_USER_ANALYTICS_QUERY, "dw_user_analytics")


def get_top_customers():
    run_query(TOP_CUSTOMERS_QUERY, "top_customers")


def get_customer_segmentation():
    run_query(CUSTOMER_SEGMENTATION_QUERY, "customer_segmentation")


def get_churn_risk():
    run_query(CHURN_RISK_QUERY, "churn_risk")


def get_revenue_usage_mismatch():
    run_query(REVENUE_USAGE_MISMATCH_QUERY, "revenue_usage_mismatch")


def build_pipeline():
    create_agg_user_revenue()
    create_agg_user_usage()
    create_agg_monthly_revenue()
    create_agg_arpu()
    create_session_buckets()
    create_agg_session_distribution()
    # Stage 4
    create_dw_user_analytics()
