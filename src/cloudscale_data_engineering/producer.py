from dotenv import load_dotenv
from kafka import KafkaProducer
from pathlib import Path
import pandas as pd
import json
import os
import time


# ============================================================
# Load environment variables
# ============================================================

load_dotenv()


# ============================================================
# Project root directory
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_FILE = PROJECT_ROOT / "data" / "quality" / "cleaned_orders.csv"


# ============================================================
# Kafka configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

TOPIC_NAME = "orders"


# ============================================================
# Create Kafka producer
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

    # Use order_id as Kafka message key
    key_serializer=lambda key: str(key).encode("utf-8"),

    # Convert Python dictionary to JSON
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),

    # Wait for acknowledgement from all in-sync replicas
    acks="all",

    # Retry temporary delivery failures
    retries=5,

    # Prevent duplicate records caused by producer retries
    enable_idempotence=True,
)


# ============================================================
# Start producer
# ============================================================

print("=" * 60)
print("Starting Olist → Kafka producer")
print("=" * 60)

print(f"Kafka server : {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Kafka topic  : {TOPIC_NAME}")
print(f"Dataset      : {DATA_FILE}")
print("=" * 60)


# ============================================================
# Validate dataset path
# ============================================================

if not DATA_FILE.exists():
    raise FileNotFoundError(
        f"Dataset not found: {DATA_FILE}"
    )


# ============================================================
# Producer counters
# ============================================================

orders_sent = 0
chunks_processed = 0

START_TIME = time.time()


# ============================================================
# Read cleaned dataset in chunks
# ============================================================

for chunk in pd.read_csv(
    DATA_FILE,
    chunksize=1000
):

    chunks_processed += 1

    print(
        f"\nProcessing chunk {chunks_processed} "
        f"({len(chunk)} rows)"
    )

    # --------------------------------------------------------
    # Process every order in the chunk
    # --------------------------------------------------------

    for _, row in chunk.iterrows():

        # Convert Pandas row into normal Python dictionary
        order = {}

        for column, value in row.items():

            if pd.isna(value):
                order[column] = None

            else:
                order[column] = value

        # ----------------------------------------------------
        # Get order ID
        # ----------------------------------------------------

        order_id = order.get("order_id")

        if not order_id:
            print("Skipping record without order_id")
            continue

        # ----------------------------------------------------
        # Send order to Kafka
        # ----------------------------------------------------

        producer.send(
            TOPIC_NAME,
            key=str(order_id),
            value=order
        )

        orders_sent += 1

        print(
            f"Sent order {orders_sent}: {order_id}"
        )

        # ----------------------------------------------------
        # Small delay to simulate streaming
        # ----------------------------------------------------

        time.sleep(0.01)

        # ----------------------------------------------------
        # Flush every 500 records
        # ----------------------------------------------------

        if orders_sent % 500 == 0:

            producer.flush()

            elapsed = time.time() - START_TIME

            print(
                f"\nCheckpoint: {orders_sent} orders sent "
                f"in {elapsed:.2f} seconds\n"
            )


# ============================================================
# Final flush
# ============================================================

producer.flush()


# ============================================================
# Statistics
# ============================================================

elapsed = time.time() - START_TIME


print("\n" + "=" * 60)
print("Producer completed successfully!")
print("=" * 60)

print(f"Total orders sent : {orders_sent}")
print(f"Chunks processed  : {chunks_processed}")
print(f"Time taken        : {elapsed:.2f} seconds")

if elapsed > 0:
    print(
        f"Average rate      : "
        f"{orders_sent / elapsed:.2f} orders/sec"
    )

print("=" * 60)


# ============================================================
# Close producer
# ============================================================

producer.close()