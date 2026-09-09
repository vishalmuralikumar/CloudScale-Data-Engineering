from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    year,
    month,
    date_format,
    desc,
)


INPUT_PATH = "/workspace/data/spark/orders_transformed"
OUTPUT_PATH = "/workspace/data/analytics"


def main():
    spark = (
        SparkSession.builder
        .appName("CloudScale-Analytics")
        .master("local[*]")
        .getOrCreate()
    )

    print("\n" + "=" * 70)
    print("CLOUDSCALE ANALYTICS")
    print("=" * 70)

    # ---------------------------------------------------------
    # Read transformed Parquet data
    # ---------------------------------------------------------

    df = spark.read.parquet(INPUT_PATH)

    print(f"\nTotal Records: {df.count()}")

    # ---------------------------------------------------------
    # 1. Orders by Status
    # ---------------------------------------------------------

    orders_by_status = (
        df.groupBy("order_status")
        .agg(count("*").alias("order_count"))
        .orderBy(desc("order_count"))
    )

    print("\n--- ORDERS BY STATUS ---")
    orders_by_status.show()

    # ---------------------------------------------------------
    # 2. Orders by Month
    # ---------------------------------------------------------

    orders_by_month = (
        df.withColumn(
            "order_month",
            date_format(col("order_purchase_timestamp"), "yyyy-MM")
        )
        .groupBy("order_month")
        .agg(count("*").alias("order_count"))
        .orderBy("order_month")
    )

    print("\n--- ORDERS BY MONTH ---")
    orders_by_month.show(20)

    # ---------------------------------------------------------
    # 3. Orders by Year
    # ---------------------------------------------------------

    orders_by_year = (
        df.withColumn(
            "order_year",
            year(col("order_purchase_timestamp"))
        )
        .groupBy("order_year")
        .agg(count("*").alias("order_count"))
        .orderBy("order_year")
    )

    print("\n--- ORDERS BY YEAR ---")
    orders_by_year.show()

    # ---------------------------------------------------------
    # 4. Delivered Orders
    # ---------------------------------------------------------

    delivered_orders = df.filter(
        col("order_status") == "delivered"
    )

    delivered_count = delivered_orders.count()

    print(f"\nDelivered Orders: {delivered_count}")

    # ---------------------------------------------------------
    # 5. Cancelled Orders
    # ---------------------------------------------------------

    cancelled_orders = df.filter(
        col("order_status") == "canceled"
    )

    cancelled_count = cancelled_orders.count()

    print(f"Cancelled Orders: {cancelled_count}")

    # ---------------------------------------------------------
    # Write analytics results
    # ---------------------------------------------------------

    orders_by_status.write.mode("overwrite").parquet(
        f"{OUTPUT_PATH}/orders_by_status"
    )

    orders_by_month.write.mode("overwrite").parquet(
        f"{OUTPUT_PATH}/orders_by_month"
    )

    orders_by_year.write.mode("overwrite").parquet(
        f"{OUTPUT_PATH}/orders_by_year"
    )

    print("\n" + "=" * 70)
    print("ANALYTICS COMPLETED SUCCESSFULLY!")
    print(f"Output written to: {OUTPUT_PATH}")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()