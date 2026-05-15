from core.db import Base, engine
import pandas as pd
import models
from pipeline import build_pipeline, load_source_data

CURRENCY_MAPPING = {
    "ngn": "NGN",
    "naira": "NGN",
    "nigerian naira": "NGN",
}


def standardize_currency(value):
    # Handle NULL values
    if pd.isna(value):
        return "NGN"

    value = str(value).strip().lower()
    return CURRENCY_MAPPING.get(value, value.upper())


def import_billing_data(billing_csv_path: str):
    print("importing billing")

    billing_df = pd.read_csv(billing_csv_path)
    # remove duplicates
    billing_df = billing_df.drop_duplicates(subset=["transaction_id"])
    # standardize currency values
    billing_df["currency"] = billing_df["currency"].apply(standardize_currency)
    billing_df.to_sql(
        "src_billing_transactions",
        engine,
        if_exists="append",
        index=False,
        chunksize=1000,
    )
    print("done with billing")


def import_customer_data(customer_csv_path: str):
    print("importing customer data")
    customer_df = pd.read_csv(customer_csv_path)
    customer_df = customer_df.drop_duplicates(subset=["customer_id"])
    customer_df.to_sql(
        "src_customers", engine, if_exists="append", index=False, chunksize=1000
    )
    print("customer data done")


def import_network_data(network_csv_path: str):
    print("importing network data")
    network_df = pd.read_csv(network_csv_path)
    network_df = network_df.drop_duplicates(subset=["session_id"])
    network_df.to_sql(
        "src_network_sessions", engine, if_exists="append", index=False, chunksize=1000
    )
    print("network data done")


def import_file_data_to_postgresql(
    customer_csv_path: str,
    billing_csv_path: str,
    network_csv_path: str,
):
    import_customer_data(customer_csv_path)
    import_billing_data(billing_csv_path)
    import_network_data(network_csv_path)


def create_postgres_tables():
    Base.metadata.create_all(bind=engine)
    print("Tables created successfully.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Load CSV files into PostgreSQL and run the BigQuery pipeline."
    )
    parser.add_argument(
        "--customers-csv",
        required=True,
        help="Path to the src_customers CSV file",
    )
    parser.add_argument(
        "--billing-csv",
        required=True,
        help="Path to the src_billing_transactions CSV file",
    )
    parser.add_argument(
        "--network-csv",
        required=True,
        help="Path to the src_network_sessions CSV file",
    )

    args = parser.parse_args()

    create_postgres_tables()
    import_file_data_to_postgresql(
        args.customers_csv, args.billing_csv, args.network_csv
    )
    load_source_data()
    build_pipeline()
