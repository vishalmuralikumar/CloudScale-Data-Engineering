from pyspark.sql import SparkSession


INPUT_PATH = "/workspace/data/spark/orders_transformed"


def main():
    spark = (
        SparkSession.builder
        .appName("CloudScale-Parquet-Verification")
        .master("local[*]")
        .getOrCreate()
    )

    print("\n" + "=" * 60)
    print("PARQUET DATA VERIFICATION")
    print("=" * 60)

    df = spark.read.parquet(INPUT_PATH)

    print(f"\nPARQUET ROW COUNT: {df.count()}")

    print("\nPARQUET SCHEMA:")
    df.printSchema()

    print("\nSAMPLE DATA:")
    df.show(5, truncate=False)

    print("=" * 60)
    print("PARQUET VERIFICATION COMPLETED SUCCESSFULLY!")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()