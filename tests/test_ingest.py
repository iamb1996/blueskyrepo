import os
from pymongo import MongoClient


MONGO_HOST = os.getenv("MONGO_HOST", "mongodb")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))

MONGO_DATABASE = os.getenv("MONGO_DATABASE", "bluesky")
MONGO_COLLECTION = os.getenv(
    "MONGO_COLLECTION",
    "bluesky_posts"
)


def test_mongodb_contains_posts():

    client = MongoClient(
        host=MONGO_HOST,
        port=MONGO_PORT,
        serverSelectionTimeoutMS=5000
    )

    # Vérifie que MongoDB est accessible
    client.admin.command("ping")

    collection = client[
        MONGO_DATABASE
    ][
        MONGO_COLLECTION
    ]

    count = collection.count_documents({})

    client.close()

    assert count > 0, (
        "MongoDB ne contient aucune donnée."
    )