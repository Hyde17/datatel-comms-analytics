## Running the application

Use command-line arguments to pass the CSV source file paths when launching the app:

```bash
python main.py \
  --customers-csv data/src_customers.csv \
  --billing-csv data/src_billing_transactions.csv \
  --network-csv data/src_network_sessions.csv
```

This loads the CSV files into PostgreSQL, then runs the pipeline to load data into BigQuery and build the analytics tables.

### Why `ROW_NUMBER()` Was Used Instead of `SELECT DISTINCT`

`ROW_NUMBER()` was chosen instead of `SELECT DISTINCT` because the requirement was not simply to remove identical rows, but to keep the most recent record for each duplicated `transaction_id`.

`SELECT DISTINCT` only removes rows that are completely identical across all selected columns. It does not provide any control over which duplicate record should be retained when duplicate transaction IDs contain different values, timestamps, or retry information.

Using:

```sql
ROW_NUMBER() OVER (
    PARTITION BY transaction_id
    ORDER BY transaction_date DESC
)

```

### Reflection on the Churn Risk Rule

The current churn detection rule is intentionally simple, but it has several limitations.

Customers are classified as **“High Risk”** when they have:

- fewer than 5 total sessions, and
- less than ₦1,000 in total revenue.

While this can help identify inactive users, it may also incorrectly flag legitimate customers. Examples include:

- newly registered customers,
- recently activated SIM cards,
- seasonal or infrequent users,
- customers who have not yet completed their first billing cycle.

For example, a customer who registered yesterday would naturally have:

- very low session activity,
- little or no revenue,
- but may still be an active customer rather than a churn risk.

Because of this, the current rule may produce false positives.

A more robust churn model would incorporate additional behavioural and temporal features, including:

- customer account age,
- recent activity trends,
- rolling 30-day usage patterns,
- historical revenue behaviour,
- changes in engagement over time.

One practical improvement would be introducing a grace period using the `created_at` field. For example, customers could only be evaluated for churn risk if their account is older than 30 days.

Example improvement:

```sql
WHEN total_sessions < 5
 AND total_revenue < 1000
 AND DATE_DIFF(CURRENT_DATE(), DATE(created_at), DAY) > 30
```

This reduces the likelihood of incorrectly flagging newly onboarded customers as churn risks.
