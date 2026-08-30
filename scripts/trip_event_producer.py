"""
Simulates a live NYC taxi trip event feed by replaying a sample of
already-ingested Bronze trip data into Event Hubs via its Kafka-
compatible endpoint. Each event is stamped with a fresh "event_time"
at send time, so downstream consumers see it as if it just happened.
"""

import json
import os
import time
import random
import logging
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from pathlib import Path
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


KEY_VAULT_URL = "https://kv-nyc-taxi-dev.vault.azure.net/"
EVENT_HUB_NAMESPACE = "ehns-nyc-taxi-dev"
EVENT_HUB_TOPIC = "trips-stream"
SAMPLE_TRIPS_PATH = "abfss://bronze@stdatalakenyctaxi.dfs.core.windows.net/trips_raw/year=2025/month=06"
SAMPLE_SIZE = 200
MIN_DELAY_SECONDS = 1.0
MAX_DELAY_SECONDS = 3.0



env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)
SP_TENANT_ID = os.getenv("SP_TENANT_ID")
SP_CLIENT_ID = os.getenv("SP_CLIENT_ID")
SP_CLIENT_SECRET = os.getenv("SP_CLIENT_SECRET")

def get_secret_from_keyvault(secret_name: str, credential) -> str:
    """Fetch a secret from Key Vault using an authenticated credential."""
    client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
    return client.get_secret(secret_name).value


def load_sample_trips(credential, sample_size: int = SAMPLE_SIZE) -> pd.DataFrame:
    """
    Load a small random sample of already-ingested Bronze trip rows to
    replay as simulated live events. Uses adlfs with the same service
    principal already granted Storage Blob Data Contributor.
    """
    storage_options = {
        "account_name": "stdatalakenyctaxi",
        "client_id": SP_CLIENT_ID,
        "client_secret": SP_CLIENT_SECRET,
        "tenant_id": SP_TENANT_ID,
    }
    df = pd.read_parquet(SAMPLE_TRIPS_PATH, storage_options=storage_options)
    return df.sample(n=min(sample_size, len(df)), random_state=42).reset_index(drop=True)


def build_producer(bootstrap_servers: str, connection_string: str) -> KafkaProducer:
    """Construct a Kafka producer configured for Event Hubs' Kafka-compatible endpoint."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username="$ConnectionString",
        sasl_plain_password=connection_string,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=3,
        acks="all",
    )


def row_to_event(row: pd.Series) -> dict:
    """Convert one sampled trip row into a JSON-serializable event payload."""
    return {
        "vendor_id": int(row["VendorID"]) if pd.notna(row["VendorID"]) else None,
        "pickup_location_id": int(row["PULocationID"]) if pd.notna(row["PULocationID"]) else None,
        "dropoff_location_id": int(row["DOLocationID"]) if pd.notna(row["DOLocationID"]) else None,
        "trip_distance": float(row["trip_distance"]) if pd.notna(row["trip_distance"]) else None,
        "fare_amount": float(row["fare_amount"]) if pd.notna(row["fare_amount"]) else None,
        "passenger_count": int(row["passenger_count"]) if pd.notna(row["passenger_count"]) else None,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "source": "simulated_live_replay",
    }


def run_producer() -> None:
    credential = ClientSecretCredential(SP_TENANT_ID, SP_CLIENT_ID, SP_CLIENT_SECRET)

    logger.info("Fetching Event Hubs send credentials from Key Vault...")
    send_conn_str = get_secret_from_keyvault("eh-send-connection-string", credential)

    logger.info("Loading sample trip data for replay...")
    trips_df = load_sample_trips(credential)
    logger.info(f"Loaded {len(trips_df)} sample trips.")

    bootstrap_servers = f"{EVENT_HUB_NAMESPACE}.servicebus.windows.net:9093"
    producer = build_producer(bootstrap_servers, send_conn_str)

    sent_count = 0
    try:
        for _, row in trips_df.iterrows():
            event = row_to_event(row)
            try:
                producer.send(EVENT_HUB_TOPIC, value=event)
                sent_count += 1
                logger.info(f"Sent event {sent_count}/{len(trips_df)}: {event}")
            except Exception as send_err:
                logger.error(f"Failed to send event: {send_err}")

            time.sleep(random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS))

    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")
    finally:
        producer.flush()
        producer.close()
        logger.info(f"Producer finished. Total events sent: {sent_count}")


if __name__ == "__main__":
    run_producer()