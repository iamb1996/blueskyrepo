import os
import requests


OLLAMA_HOST = os.getenv(
    "OLLAMA_HOST",
    "http://ollama:11434"
)

EMBEDDING_MODEL = os.getenv(
    "EMBEDDING_MODEL",
    "qwen3-embedding:0.6b"
)


def test_embedding_service():

    response = requests.post(
        f"{OLLAMA_HOST}/api/embed",
        json={
            "model": EMBEDDING_MODEL,
            "input": "This is a test document."
        },
        timeout=120
    )

    assert response.status_code == 200

    data = response.json()

    assert "embeddings" in data

    embeddings = data["embeddings"]

    assert len(embeddings) > 0

    vector = embeddings[0]

    assert isinstance(vector, list)

    assert len(vector) > 0

    assert all(
        isinstance(value, (int, float))
        for value in vector
    )

    print(
        f"\nEmbedding model: {EMBEDDING_MODEL}"
    )

    print(
        f"Embedding dimension: {len(vector)}"
    )