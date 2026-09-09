from dotenv import load_dotenv
from kafka import KafkaConsumer
import json
import os
import psycopg


# Load environment variables from .env
load_dotenv()


# Create Kafka consumer
consumer = KafkaConsumer(
    "orders",
    bootstrap_servers=os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092"
    ),
    group_id="postgres-orders-consumer",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),
    key_deserializer=lambda key: (
        key.decode("utf-8") if key else None
    ),
)


# Connect to PostgreSQL
connection = psycopg.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5433")),
    dbname=os.getenv("POSTGRES_DB", "cloudscale"),
    user=os.getenv("POSTGRES_USER", "cloudscale"),
    password=os.getenv("POSTGRES_PASSWORD", "cloudscale_pass"),
)

cursor = connection.cursor()

print("Connected to PostgreSQL successfully!")
print("Starting Kafka consumer...")
print("Listening to topic: orders")


# Read messages from Kafka
for message in consumer:

    print("\n--- Message Received ---")
    print(f"Partition: {message.partition}")
    print(f"Offset: {message.offset}")
    print(f"Key: {message.key}")
    print(f"Value: {message.value}")

    order = message.value

    # Validate required Olist fields
    required_fields = [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    if not all(field in order for field in required_fields):
        print(
            f"Skipping incompatible message: "
            f"{order.get('order_id', 'UNKNOWN')}"
        )
        continue

    # Insert Kafka message into PostgreSQL
    cursor.execute(
        """
        INSERT INTO orders (
            order_id,
            customer_id,
            order_status,
            order_purchase_timestamp,
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
        ON CONFLICT (order_id) DO NOTHING
        """,
        (
            order["order_id"],
            order["customer_id"],
            order["order_status"],
            order["order_purchase_timestamp"],
            order["order_approved_at"],
            order["order_delivered_carrier_date"],
            order["order_delivered_customer_date"],
            order["order_estimated_delivery_date"],
        ),
    )

    connection.commit()

    print(f"Inserted order into PostgreSQL: {order['order_id']}")