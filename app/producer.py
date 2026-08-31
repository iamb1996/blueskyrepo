import json
import os
import time

from atproto import (
    CAR,
    FirehoseSubscribeReposClient,
    parse_subscribe_repos_message,
)
from kafka import KafkaProducer


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


# ============================================================
# KAFKA
# ============================================================

def create_kafka_producer():

    while True:

        try:

            print(
                "[PRODUCER] Connecting to Kafka...",
                flush=True,
            )

            producer = KafkaProducer(

                bootstrap_servers=(
                    KAFKA_BOOTSTRAP_SERVERS
                ),

                value_serializer=lambda value:
                    json.dumps(
                        value,
                        ensure_ascii=False,
                    ).encode("utf-8"),

                acks=1,

                linger_ms=5,

                batch_size=65536,

                compression_type="gzip",

                retries=10,

                max_in_flight_requests_per_connection=5,

                request_timeout_ms=30000,

            )

            print(
                "[PRODUCER] Kafka connected",
                flush=True,
            )

            return producer

        except Exception as e:

            print(
                f"[PRODUCER] Kafka unavailable: {e}",
                flush=True,
            )

            time.sleep(3)


producer = create_kafka_producer()


# ============================================================
# STATISTICS
# ============================================================

total_posts = 0
total_errors = 0

start_time = time.time()

last_report_time = time.time()
last_report_count = 0


# ============================================================
# STATISTICS
# ============================================================

def print_stats():

    global last_report_time
    global last_report_count

    now = time.time()

    if now - last_report_time < 5:

        return

    elapsed = (
        now - last_report_time
    )

    current_rate = (
        total_posts
        - last_report_count
    ) / max(elapsed, 1)

    average_rate = (
        total_posts
        / max(
            now - start_time,
            1,
        )
    )

    print(
        f"[PRODUCER] "
        f"rate={current_rate:.1f} posts/s | "
        f"total={total_posts} | "
        f"avg={average_rate:.1f} posts/s | "
        f"errors={total_errors}",
        flush=True,
    )

    last_report_time = now
    last_report_count = total_posts


# ============================================================
# FIREHOSE MESSAGE
# ============================================================

def handle_message(message):

    global total_posts
    global total_errors

    try:

        commit = parse_subscribe_repos_message(
            message
        )

        # ----------------------------------------------------
        # IMPORTANT
        #
        # Firehose peut envoyer :
        # Identity
        # Account
        # Sync
        # Commit
        #
        # Seul Commit contient ops.
        # ----------------------------------------------------

        if commit is None:

            return

        # Vérification générique
        if not hasattr(
            commit,
            "ops",
        ):

            return

        if not hasattr(
            commit,
            "blocks",
        ):

            return

        if not commit.ops:

            return

        if not commit.blocks:

            return


        # ----------------------------------------------------
        # CAR
        # ----------------------------------------------------

        car = CAR.from_bytes(
            commit.blocks
        )


        # ----------------------------------------------------
        # OPERATIONS
        # ----------------------------------------------------

        for op in commit.ops:

            try:

                # --------------------------------------------
                # Seulement CREATE
                # --------------------------------------------

                if op.action != "create":

                    continue


                # --------------------------------------------
                # Seulement les posts Bluesky
                # --------------------------------------------

                if not op.path.startswith(
                    "app.bsky.feed.post/"
                ):

                    continue


                if not op.cid:

                    continue


                # --------------------------------------------
                # Récupérer le record
                # --------------------------------------------

                record = car.blocks.get(
                    op.cid
                )


                if record is None:

                    continue


                # --------------------------------------------
                # Certains objets peuvent être bytes
                # --------------------------------------------

                if isinstance(
                    record,
                    bytes,
                ):

                    try:

                        record = json.loads(
                            record.decode(
                                "utf-8"
                            )
                        )

                    except Exception:

                        continue


                if not isinstance(
                    record,
                    dict,
                ):

                    continue


                # --------------------------------------------
                # Vérifier le type
                # --------------------------------------------

                if record.get(
                    "$type"
                ) != "app.bsky.feed.post":

                    continue


                # --------------------------------------------
                # Texte
                # --------------------------------------------

                text = record.get(
                    "text"
                )


                if not isinstance(
                    text,
                    str,
                ):

                    continue


                text = text.strip()


                if not text:

                    continue


                # --------------------------------------------
                # POST
                # --------------------------------------------

                post = {

                    "uri": (
                        f"at://{commit.repo}/"
                        f"{op.path}"
                    ),

                    "cid": str(
                        op.cid
                    ),

                    "author_did": (
                        commit.repo
                    ),

                    "text": text,

                    "created_at": (
                        record.get(
                            "createdAt"
                        )
                    ),

                    "ingested_at": (
                        time.time()
                    ),
                }


                # --------------------------------------------
                # Kafka
                # --------------------------------------------

                producer.send(
                    KAFKA_TOPIC,
                    value=post,
                )

                total_posts += 1


                # --------------------------------------------
                # Log
                # --------------------------------------------

                if total_posts <= 20:

                    print(
                        "[PRODUCER] Sent | "
                        f"{commit.repo} | "
                        f"{text[:100]}",
                        flush=True,
                    )


            except Exception as e:

                total_errors += 1

                print(
                    f"[PRODUCER] Operation error: {e}",
                    flush=True,
                )


        print_stats()


    except Exception as e:

        total_errors += 1

        # Ne pas spammer les logs avec les messages
        # non-Commit du Firehose.

        if "ops" not in str(e):

            print(
                f"[PRODUCER] Processing error: {e}",
                flush=True,
            )


# ============================================================
# FIREHOSE
# ============================================================

def run_firehose():

    while True:

        firehose = None

        try:

            print(
                "[PRODUCER] Connecting to "
                "Bluesky Firehose...",
                flush=True,
            )

            firehose = (
                FirehoseSubscribeReposClient()
            )

            print(
                "[PRODUCER] Connected to "
                "Bluesky Firehose",
                flush=True,
            )

            firehose.start(
                handle_message
            )

        except Exception as e:

            print(
                f"[PRODUCER] Firehose disconnected: {e}",
                flush=True,
            )

            print(
                "[PRODUCER] Reconnecting in 3 seconds...",
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
        "       BLUESKY FIREHOSE PRODUCER",
        flush=True,
    )

    print(
        "==========================================",
        flush=True,
    )

    print(
        f"[PRODUCER] Kafka: "
        f"{KAFKA_BOOTSTRAP_SERVERS}",
        flush=True,
    )

    print(
        f"[PRODUCER] Topic: "
        f"{KAFKA_TOPIC}",
        flush=True,
    )


    run_firehose()


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    main()
