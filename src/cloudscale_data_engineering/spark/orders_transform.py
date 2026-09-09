from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp


# ============================================================
# Create Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("CloudScale-Orders-Transformation")
    .master("local[*]")
    .getOrCreate()
)

print("=" * 60)
print("CloudScale PySpark Transformation Started")
print("=" * 60)


# ============================================================
# Project paths
# ============================================================

INPUT_PATH = "data/quality/cleaned_orders.csv"

OUTPUT_PATH = "data/spark/orders_transformed"


# ============================================================
# Read CSV
# ============================================================

orders_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(INPUT_PATH)
)


# ============================================================
# Show input data
# ============================================================

print("\nInput Schema:")
orders_df.printSchema()

print("\nInput Record Count:")
print(orders_df.count())


# ============================================================
# Convert timestamp columns
# ============================================================

timestamp_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

for column_name in timestamp_columns:

    orders_df = orders_df.withColumn(
        column_name,
        to_timestamp(col(column_name))
    )


# ============================================================
# Select required columns
# ============================================================

transformed_df = orders_df.select(
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
)


# ============================================================
# Remove duplicate orders
# ============================================================

transformed_df = transformed_df.dropDuplicates(
    ["order_id"]
)


# ============================================================
# Show transformed data
# ============================================================

print("\nTransformed Schema:")
transformed_df.printSchema()

print("\nTransformed Record Count:")
print(transformed_df.count())

print("\nSample Records:")
transformed_df.show(5, truncate=False)


# ============================================================
# Write transformed data
# ============================================================

(
    transformed_df.write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)


print("\nTransformation completed successfully!")

print(f"Output written to: {OUTPUT_PATH}")


# ============================================================
# Stop Spark
# ============================================================

spark.stop()