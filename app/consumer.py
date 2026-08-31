import json
import os
import time

from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import BulkWriteError


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "bluesky-posts",
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "bluesky-consumer",
)

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://mongodb:27017",
)

MONGO_DATABASE = os.getenv(
    "MONGO_DATABASE",
    "bluesky",
)

MONGO_COLLECTION = os.getenv(
    "MONGO_COLLECTION",
    "posts",
)


# ============================================================
# MONGODB
# ============================================================

def connect_mongodb():

    while True:

        try:

            print(
                "[CONSUMER] Connecting to MongoDB...",
                flush=True,
            )

            client = MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
            )

            client.admin.command(
                "ping"
            )

            database = client[
                MONGO_DATABASE
            ]

            collection = database[
                MONGO_COLLECTION
            ]

            print(
                "[CONSUMER] MongoDB connected",
                flush=True,
            )

            return client, collection

        except Exception as e:

            print(
                f"[CONSUMER] MongoDB unavailable: {e}",
                flush=True,
            )

            time.sleep(3)


# ============================================================
# KAFKA
# ============================================================

def connect_kafka():

    while True:

        try:

            print(
                "[CONSUMER] Connecting to Kafka...",
                flush=True,
            )

            consumer = KafkaConsumer(

                KAFKA_TOPIC,

                bootstrap_servers=(
                    KAFKA_BOOTSTRAP_SERVERS
                ),

                group_id=KAFKA_GROUP_ID,

                auto_offset_reset="earliest",

                enable_auto_commit=False,

                value_deserializer=lambda value:
                    json.loads(
                        value.decode("utf-8")
                    ),

                max_poll_records=500,

                fetch_min_bytes=1,

                fetch_max_wait_ms=100,

                request_timeout_ms=30000,

                session_timeout_ms=10000,

                heartbeat_interval_ms=3000,

            )

            print(
                "[CONSUMER] Kafka connected",
                flush=True,
            )

            return consumer

        except Exception as e:

            print(
                f"[CONSUMER] Kafka unavailable: {e}",
                flush=True,
            )

            time.sleep(3)


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "==========================================",
        flush=True,
    )

    print(
        "          BLUESKY KAFKA CONSUMER",
        flush=True,
    )

    print(
        "==========================================",
        flush=True,
    )

    print(
        f"[CONSUMER] Kafka: "
        f"{KAFKA_BOOTSTRAP_SERVERS}",
        flush=True,
    )

    print(
        f"[CONSUMER] Topic: "
        f"{KAFKA_TOPIC}",
        flush=True,
    )

    print(
        f"[CONSUMER] Group: "
        f"{KAFKA_GROUP_ID}",
        flush=True,
    )

    print(
        f"[CONSUMER] MongoDB: "
        f"{MONGO_DATABASE}."
        f"{MONGO_COLLECTION}",
        flush=True,
    )


    # ========================================================
    # MONGODB
    # ========================================================

    mongo_client, collection = (
        connect_mongodb()
    )


    # ========================================================
    # INDEX
    # ========================================================

    try:

        collection.create_index(
            "uri",
            unique=True,
        )

        print(
            "[CONSUMER] MongoDB index ready",
            flush=True,
        )

    except Exception as e:

        print(
            f"[CONSUMER] Index warning: {e}",
            flush=True,
        )


    # ========================================================
    # KAFKA
    # ========================================================

    consumer = connect_kafka()


    # ========================================================
    # STATISTICS
    # ========================================================

    total_messages = 0
    total_inserted = 0
    total_duplicates = 0
    total_errors = 0

    start_time = time.time()

    last_report_time = time.time()
    last_report_count = 0


    # ========================================================
    # CONSUMER LOOP
    # ========================================================

    try:

        while True:

            records = consumer.poll(
                timeout_ms=1000,
                max_records=500,
            )


            if not records:

                continue


            batch = []


            # =================================================
            # READ KAFKA
            # =================================================

            for _, messages in records.items():

                for message in messages:

                    try:

                        post = message.value


                        if not isinstance(
                            post,
                            dict,
                        ):

                            continue


                        if not post.get(
                            "uri"
                        ):

                            continue


                        if not post.get(
                            "text"
                        ):

                            continue


                        post.pop(
                            "_id",
                            None,
                        )


                        batch.append(
                            post
                        )

                        total_messages += 1


                    except Exception as e:

                        total_errors += 1

                        print(
                            f"[CONSUMER] "
                            f"Message error: {e}",
                            flush=True,
                        )


            # =================================================
            # MONGODB
            # =================================================

            if batch:

                try:

                    result = (
                        collection.insert_many(
                            batch,
                            ordered=False,
                        )
                    )

                    inserted = len(
                        result.inserted_ids
                    )

                    total_inserted += (
                        inserted
                    )


                    # -----------------------------------------
                    # Commit Kafka AFTER MongoDB
                    # -----------------------------------------

                    consumer.commit()


                except BulkWriteError as e:

                    details = (
                        e.details or {}
                    )

                    write_errors = (
                        details.get(
                            "writeErrors",
                            [],
                        )
                    )


                    duplicate_count = sum(

                        1

                        for error
                        in write_errors

                        if error.get(
                            "code"
                        ) == 11000

                    )


                    other_errors = (
                        len(write_errors)
                        - duplicate_count
                    )


                    total_duplicates += (
                        duplicate_count
                    )


                    if other_errors == 0:

                        total_inserted += (
                            len(batch)
                            - duplicate_count
                        )

                        consumer.commit()

                    else:

                        total_errors += (
                            other_errors
                        )

                        print(
                            "[CONSUMER] "
                            "MongoDB bulk error",
                            flush=True,
                        )


                except Exception as e:

                    total_errors += 1

                    print(
                        f"[CONSUMER] "
                        f"MongoDB error: {e}",
                        flush=True,
                    )


            # =================================================
            # STATISTICS
            # =================================================

            now = time.time()


            if (
                now - last_report_time
                >= 5
            ):

                elapsed = (
                    now
                    - last_report_time
                )


                rate = (

                    total_messages
                    - last_report_count

                ) / max(
                    elapsed,
                    1,
                )


                average = (

                    total_messages
                    / max(
                        now - start_time,
                        1,
                    )

                )


                print(

                    f"[CONSUMER] "
                    f"rate={rate:.1f} msg/s | "
                    f"total={total_messages} | "
                    f"inserted={total_inserted} | "
                    f"duplicates={total_duplicates} | "
                    f"avg={average:.1f} msg/s | "
                    f"errors={total_errors}",

                    flush=True,
                )


                last_report_time = now

                last_report_count = (
                    total_messages
                )


    except KeyboardInterrupt:

        print(
            "[CONSUMER] Stopping...",
            flush=True,
        )


    finally:

        try:

            consumer.close()

        except Exception:

            pass


        try:

            mongo_client.close()

        except Exception:

            pass


        print(
            "[CONSUMER] Closed",
            flush=True,
        )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    main()
