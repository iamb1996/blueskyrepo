import os
import time
import uuid

from pymongo import MongoClient
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    PointStruct,
)


# ============================================================
# CONFIGURATION
# ============================================================

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://mongodb:27017"
)

MONGO_DATABASE = os.getenv(
    "MONGO_DATABASE",
    "bluesky"
)

MONGO_COLLECTION = os.getenv(
    "MONGO_COLLECTION",
    "posts"
)

QDRANT_HOST = os.getenv(
    "QDRANT_HOST",
    "qdrant"
)

QDRANT_PORT = int(
    os.getenv(
        "QDRANT_PORT",
        "6333"
    )
)

QDRANT_COLLECTION = os.getenv(
    "QDRANT_COLLECTION",
    "bluesky_posts"
)

BATCH_SIZE = int(
    os.getenv(
        "QDRANT_BATCH_SIZE",
        "100"
    )
)

POLL_INTERVAL = float(
    os.getenv(
        "QDRANT_POLL_INTERVAL",
        "2"
    )
)


# ============================================================
# MONGODB
# ============================================================

def connect_mongodb():

    while True:

        try:

            print(
                "[QDRANT] Connecting to MongoDB...",
                flush=True
            )

            mongo = MongoClient(

                MONGO_URI,

                serverSelectionTimeoutMS=5000
            )

            mongo.admin.command(
                "ping"
            )

            db = mongo[
                MONGO_DATABASE
            ]

            collection = db[
                MONGO_COLLECTION
            ]

            print(
                "[QDRANT] MongoDB connected",
                flush=True
            )

            return mongo, collection

        except Exception as e:

            print(
                f"[QDRANT] MongoDB unavailable: "
                f"{e}",
                flush=True
            )

            time.sleep(5)


# ============================================================
# QDRANT
# ============================================================

def connect_qdrant():

    while True:

        try:

            print(
                f"[QDRANT] Connecting to "
                f"{QDRANT_HOST}:{QDRANT_PORT}",
                flush=True
            )

            qdrant = QdrantClient(

                host=QDRANT_HOST,

                port=QDRANT_PORT
            )

            qdrant.get_collections()

            print(
                "[QDRANT] Connected",
                flush=True
            )

            return qdrant

        except Exception as e:

            print(
                f"[QDRANT] Qdrant unavailable: "
                f"{e}",
                flush=True
            )

            time.sleep(5)


# ============================================================
# ENSURE COLLECTION
# ============================================================

def ensure_collection(
    qdrant,
    vector_size
):

    collections = qdrant.get_collections()

    names = [

        c.name

        for c in collections.collections
    ]


    # --------------------------------------------------------
    # CREATE COLLECTION
    # --------------------------------------------------------

    if QDRANT_COLLECTION not in names:

        print(

            f"[QDRANT] Creating collection "
            f"{QDRANT_COLLECTION} "
            f"dimension={vector_size}",

            flush=True
        )

        qdrant.create_collection(

            collection_name=
                QDRANT_COLLECTION,

            vectors_config=
                VectorParams(

                    size=vector_size,

                    distance=Distance.COSINE
                )
        )

        print(
            "[QDRANT] Collection created",
            flush=True
        )

        return


    # --------------------------------------------------------
    # CHECK DIMENSION
    # --------------------------------------------------------

    info = qdrant.get_collection(
        QDRANT_COLLECTION
    )

    existing_size = (
        info.config.params.vectors.size
    )


    if existing_size != vector_size:

        raise ValueError(

            "Vector dimension mismatch: "

            f"Qdrant collection = "
            f"{existing_size}, "

            f"embedding = "
            f"{vector_size}"
        )


# ============================================================
# POINT ID
# ============================================================

def get_point_id(
    document
):

    return str(

        uuid.uuid5(

            uuid.NAMESPACE_URL,

            str(document["_id"])
        )
    )


# ============================================================
# STORE BATCH
# ============================================================

def store_batch(
    qdrant,
    collection,
    points,
    mongo_ids
):

    if not points:

        return 0


    # --------------------------------------------------------
    # STORE IN QDRANT
    # --------------------------------------------------------

    qdrant.upsert(

        collection_name=
            QDRANT_COLLECTION,

        points=points,

        wait=True
    )


    # --------------------------------------------------------
    # MARK MONGODB DOCUMENTS
    # --------------------------------------------------------

    collection.update_many(

        {
            "_id": {
                "$in": mongo_ids
            }
        },

        {
            "$set": {

                "qdrant_stored": True,

                "qdrant_stored_at":
                    time.time()
            }
        }
    )


    return len(points)


# ============================================================
# MAIN
# ============================================================

def run():

    mongo, collection = connect_mongodb()

    qdrant = connect_qdrant()


    total_stored = 0

    total_errors = 0


    print(
        "==========================================",
        flush=True
    )

    print(
        "       BLUESKY QDRANT SERVICE",
        flush=True
    )

    print(
        "==========================================",
        flush=True
    )

    print(
        f"[QDRANT] Collection: "
        f"{QDRANT_COLLECTION}",
        flush=True
    )

    print(
        f"[QDRANT] Batch size: "
        f"{BATCH_SIZE}",
        flush=True
    )

    print(
        "[QDRANT] Running...",
        flush=True
    )


    while True:

        try:

            documents = collection.find(

                {
                    "embedded": True,

                    "embedding": {
                        "$exists": True
                    },

                    "qdrant_stored": {
                        "$ne": True
                    }
                },

                {
                    "embedding": 1,
                    "text": 1,
                    "cleaned_text": 1,
                    "language": 1,
                    "uri": 1,
                    "cid": 1,
                    "author_did": 1,
                    "created_at": 1,
                    "ingested_at": 1,
                    "embedding_model": 1
                },

                batch_size=BATCH_SIZE
            )


            points = []

            mongo_ids = []


            for document in documents:

                try:

                    vector = document.get(
                        "embedding"
                    )


                    if not vector:

                        continue


                    # ------------------------------------------------
                    # VERIFY COLLECTION
                    # ------------------------------------------------

                    ensure_collection(

                        qdrant,

                        len(vector)
                    )


                    # ------------------------------------------------
                    # PAYLOAD
                    # ------------------------------------------------

                    payload = {

                        "text":
                            document.get(
                                "text"
                            ),

                        "cleaned_text":
                            document.get(
                                "cleaned_text"
                            ),

                        "language":
                            document.get(
                                "language"
                            ),

                        "uri":
                            document.get(
                                "uri"
                            ),

                        "cid":
                            document.get(
                                "cid"
                            ),

                        "author_did":
                            document.get(
                                "author_did"
                            ),

                        "created_at":
                            document.get(
                                "created_at"
                            ),

                        "ingested_at":
                            document.get(
                                "ingested_at"
                            ),

                        "embedding_model":
                            document.get(
                                "embedding_model"
                            )
                    }


                    # ------------------------------------------------
                    # POINT
                    # ------------------------------------------------

                    point = PointStruct(

                        id=get_point_id(
                            document
                        ),

                        vector=vector,

                        payload=payload
                    )


                    points.append(
                        point
                    )

                    mongo_ids.append(
                        document["_id"]
                    )


                    # ------------------------------------------------
                    # FULL BATCH
                    # ------------------------------------------------

                    if len(points) >= BATCH_SIZE:

                        stored = store_batch(

                            qdrant,

                            collection,

                            points,

                            mongo_ids
                        )


                        total_stored += stored


                        print(

                            f"[QDRANT] "
                            f"Stored: {stored} | "
                            f"Total: {total_stored}",

                            flush=True
                        )


                        points = []

                        mongo_ids = []


                except Exception as e:

                    total_errors += 1

                    print(

                        f"[QDRANT] "
                        f"Document error: {e}",

                        flush=True
                    )


            # ====================================================
            # LAST BATCH
            # ====================================================

            if points:

                stored = store_batch(

                    qdrant,

                    collection,

                    points,

                    mongo_ids
                )


                total_stored += stored


                print(

                    f"[QDRANT] "
                    f"Stored: {stored} | "
                    f"Total: {total_stored}",

                    flush=True
                )


            time.sleep(
                POLL_INTERVAL
            )


        except Exception as e:

            total_errors += 1

            print(

                f"[QDRANT] "
                f"Service error: {e}",

                flush=True
            )

            time.sleep(5)

            # Reconnexion
            mongo, collection = (
                connect_mongodb()
            )

            qdrant = connect_qdrant()


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    run()