import csv
from sqlalchemy import inspect
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


def import_billing_data():
    print("importing billing")

    billing_df = pd.read_csv("data/src_billing_transactions.csv")
    # remove duplicates
    billing_df = billing_df.drop_duplicates(subset=["transaction_id"])
    # standardize currency. values
    billing_df["currency"] = billing_df["currency"].apply(standardize_currency)
    billing_df.to_sql(
        "src_billing_transactions",
        engine,
        if_exists="append",
        index=False,
        chunksize=1000,
    )
    print("done with billing")


def import_customer_data():
    print("importing customer data")
    customer_df = pd.read_csv("./data/src_customers.csv")
    customer_df = customer_df.drop_duplicates(subset=["customer_id"])
    customer_df.to_sql(
        "src_customers", engine, if_exists="append", index=False, chunksize=1000
    )
    print("customer data done")


def import_network_data():
    print("importing network data")
    network_df = pd.read_csv("data/src_network_sessions.csv")
    network_df = network_df.drop_duplicates(subset=["session_id"])
    network_df.to_sql(
        "src_network_sessions", engine, if_exists="append", index=False, chunksize=1000
    )
    print("network data done")


def import_file_data_to_postgresql():
    import_customer_data()
    import_billing_data()
    import_network_data()


def create_postgres_tables():
    Base.metadata.create_all(bind=engine)
    print("Tables created successfully.")


if __name__ == "__main__":
    create_postgres_tables()

    import_file_data_to_postgresql()
    load_source_data()
    build_pipeline()
