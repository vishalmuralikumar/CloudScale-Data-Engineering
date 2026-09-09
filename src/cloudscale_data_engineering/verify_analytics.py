from pyspark.sql import SparkSession


BASE_PATH = "/workspace/data/analytics"


def main():
    spark = (
        SparkSession.builder
        .appName("CloudScale-Analytics-Verification")
        .master("local[*]")
        .getOrCreate()
    )

    print("\n" + "=" * 70)
    print("CLOUDSCALE ANALYTICS VERIFICATION")
    print("=" * 70)

    # Orders by Status
    print("\n--- ORDERS BY STATUS ---")
    status_df = spark.read.parquet(
        f"{BASE_PATH}/orders_by_status"
    )
    status_df.show(truncate=False)

    # Orders by Month
    print("\n--- ORDERS BY MONTH ---")
    month_df = spark.read.parquet(
        f"{BASE_PATH}/orders_by_month"
    )
    month_df.show(20, truncate=False)

    # Orders by Year
    print("\n--- ORDERS BY YEAR ---")
    year_df = spark.read.parquet(
        f"{BASE_PATH}/orders_by_year"
    )
    year_df.show(truncate=False)

    print("\n" + "=" * 70)
    print("ANALYTICS VERIFICATION COMPLETED SUCCESSFULLY!")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()