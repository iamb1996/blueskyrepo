import os
import uuid

from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    PointStruct
)


QDRANT_HOST = os.getenv(
    "QDRANT_HOST",
    "qdrant"
)

QDRANT_PORT = int(
    os.getenv("QDRANT_PORT", "6333")
)

TEST_COLLECTION = "pytest_test_collection"


def test_qdrant_connection():

    client = QdrantClient(
        host=QDRANT_HOST,
        port=QDRANT_PORT
    )

    collections = client.get_collections()

    assert collections is not None


def test_qdrant_insert_and_retrieve():

    client = QdrantClient(
        host=QDRANT_HOST,
        port=QDRANT_PORT
    )

    collections = client.get_collections().collections

    collection_names = [
        collection.name
        for collection in collections
    ]

    if TEST_COLLECTION not in collection_names:

        client.create_collection(
            collection_name=TEST_COLLECTION,
            vectors_config=VectorParams(
                size=4,
                distance=Distance.COSINE
            )
        )

    point_id = str(uuid.uuid4())

    vector = [
        0.1,
        0.2,
        0.3,
        0.4
    ]

    client.upsert(
        collection_name=TEST_COLLECTION,
        points=[
            PointStruct(
                id=point_id,
                vector=vector,
                payload={
                    "text": "pytest test document"
                }
            )
        ]
    )

    result = client.retrieve(
        collection_name=TEST_COLLECTION,
        ids=[point_id]
    )

    assert len(result) == 1

    assert result[0].payload["text"] == (
        "pytest test document"
    )