from pathlib import Path
import pandas as pd


# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Input and output paths
INPUT_FILE = PROJECT_ROOT / "data" / "raw" / "olist_orders_dataset.csv"
OUTPUT_FILE = PROJECT_ROOT / "data" / "quality" / "cleaned_orders.csv"


def clean_orders_data():
    print("Starting order data cleaning...")
    print(f"Reading raw dataset: {INPUT_FILE}")

    # Load raw dataset
    df = pd.read_csv(INPUT_FILE)

    # Convert timestamp columns
    timestamp_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    for column in timestamp_columns:
        df[column] = pd.to_datetime(df[column], errors="coerce")

    # Remove duplicate orders
    before_count = len(df)

    df = df.drop_duplicates(
        subset=["order_id"],
        keep="first"
    )

    after_count = len(df)

    print(f"Duplicate orders removed: {before_count - after_count}")
    
        # Handle invalid delivered orders
    print("\n--- Invalid Delivered Orders ---")

    invalid_delivered_orders = df[
        (df["order_status"] == "delivered") &
        (df["order_delivered_customer_date"].isna())
    ]

    print(
        f"Invalid delivered orders found: "
        f"{len(invalid_delivered_orders)}"
    )

    # Remove invalid records from cleaned dataset
    df = df.drop(
        invalid_delivered_orders.index
    )

    # Save invalid records separately for audit
    REJECTED_FILE = (
        PROJECT_ROOT
        / "data"
        / "quality"
        / "rejected_orders.csv"
    )

    invalid_delivered_orders.to_csv(
        REJECTED_FILE,
        index=False
    )

    print(
        f"Rejected records saved to: {REJECTED_FILE}"
    )

    # Create quality directory if it does not exist
    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    # Save cleaned dataset
    df.to_csv(
        OUTPUT_FILE,
        index=False
    )

    print(f"Cleaned dataset saved to: {OUTPUT_FILE}")
    print(f"Final rows: {len(df)}")


if __name__ == "__main__":
    clean_orders_data()