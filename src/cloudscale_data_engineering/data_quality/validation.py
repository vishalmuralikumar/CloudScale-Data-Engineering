from pathlib import Path
import pandas as pd


# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Raw Olist orders dataset
DATA_FILE = PROJECT_ROOT / "data" / "raw" / "olist_orders_dataset.csv"


def validate_orders_data():
    print("Starting data quality validation...")
    print(f"Reading dataset: {DATA_FILE}")

    # Check whether the dataset exists
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"Dataset not found: {DATA_FILE}")

    # Load dataset
    df = pd.read_csv(DATA_FILE)

    print("\n--- Dataset Overview ---")
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")

    # Required columns
    required_columns = [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    missing_columns = [
        column for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        print(f"Missing columns: {missing_columns}")
    else:
        print("Required columns: PASS")

    # Missing values
    print("\n--- Missing Values ---")
    missing_values = df.isnull().sum()

    for column, count in missing_values.items():
        print(f"{column}: {count}")
    
    # Duplicate order IDs
    if "order_id" in df.columns:
        duplicate_orders = df["order_id"].duplicated().sum()
        print("\n--- Duplicate Check ---")
        print(f"Duplicate order_id records: {duplicate_orders}")
    else:
        print("\n--- Duplicate Check ---")
        print("Duplicate order_id records: NOT CHECKED (column missing)")
   
    # Business rule validation
    print("\n--- Business Rule Check ---")

    invalid_delivered_orders = df[
        (df["order_status"] == "delivered")
        & (df["order_delivered_customer_date"].isnull())
    ]

    print(
        "Delivered orders with missing customer delivery date: "
        f"{len(invalid_delivered_orders)}"
    )
        # Timestamp consistency check
    print("\n--- Timestamp Consistency Check ---")

    df["order_purchase_timestamp"] = pd.to_datetime(
        df["order_purchase_timestamp"],
        errors="coerce"
    )

    df["order_approved_at"] = pd.to_datetime(
        df["order_approved_at"],
        errors="coerce"
    )

    invalid_approval_dates = df[
        (df["order_approved_at"].notna()) &
        (df["order_approved_at"] < df["order_purchase_timestamp"])
    ]

    print(
        f"Orders approved before purchase: "
        f"{len(invalid_approval_dates)}"
    )
        # Data type validation
    print("\n--- Data Type Check ---")

    string_columns = [
        "order_id",
        "customer_id",
        "order_status",
    ]

    datetime_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
    ]

    for column in string_columns:
        if column in df.columns:
            if pd.api.types.is_string_dtype(df[column]):
                print(f"{column}: PASS")
            else:
                print(f"{column}: FAIL")

    for column in datetime_columns:
        if column in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[column]):
                print(f"{column}: PASS")
            else:
                print(f"{column}: FAIL")
       
    
    # Final status
    print("\nData quality validation completed.")


if __name__ == "__main__":
    validate_orders_data()