import os
import time

from pymongo import MongoClient
from ollama import Client


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

OLLAMA_HOST = os.getenv(
    "OLLAMA_HOST",
    "http://ollama:11434"
)

# ============================================================
# IMPORTANT
# ============================================================
# Le seul modèle utilisé par ce service est :
#
#     nomic-embed-text
#
# Ne pas mettre :
#
#     sentence-transformers/all-MiniLM-L6-v2
#
# ============================================================

EMBEDDING_MODEL = os.getenv(
    "EMBEDDING_MODEL",
    "nomic-embed-text"
)

POLL_INTERVAL = float(
    os.getenv(
        "EMBEDDING_POLL_INTERVAL",
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
                "[EMBEDDING] Connecting to MongoDB...",
                flush=True
            )

            mongo = MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=5000
            )

            mongo.admin.command("ping")

            db = mongo[MONGO_DATABASE]

            collection = db[MONGO_COLLECTION]

            print(
                "[EMBEDDING] MongoDB connected",
                flush=True
            )

            return mongo, collection

        except Exception as e:

            print(
                f"[EMBEDDING] MongoDB unavailable: {e}",
                flush=True
            )

            time.sleep(5)


# ============================================================
# OLLAMA
# ============================================================

def connect_ollama():

    print(
        f"[EMBEDDING] Connecting to Ollama: "
        f"{OLLAMA_HOST}",
        flush=True
    )

    client = Client(
        host=OLLAMA_HOST
    )

    while True:

        try:

            client.show(
                EMBEDDING_MODEL
            )

            print(
                "[EMBEDDING] Ollama connected",
                flush=True
            )

            print(
                f"[EMBEDDING] Model available: "
                f"{EMBEDDING_MODEL}",
                flush=True
            )

            return client

        except Exception as e:

            print(
                f"[EMBEDDING] Model "
                f"{EMBEDDING_MODEL} not available: "
                f"{e}",
                flush=True
            )

            print(
                f"[EMBEDDING] Please pull "
                f"{EMBEDDING_MODEL} in Ollama.",
                flush=True
            )

            time.sleep(5)


# ============================================================
# CREATE EMBEDDING
# ============================================================

def create_embedding(
    ollama,
    text
):

    if not text:

        raise ValueError(
            "Empty text"
        )

    response = ollama.embed(

        model=EMBEDDING_MODEL,

        input=text
    )

    embeddings = response.get(
        "embeddings"
    )

    if not embeddings:

        raise ValueError(
            "Ollama returned no embedding"
        )

    vector = embeddings[0]

    if not vector:

        raise ValueError(
            "Empty embedding vector"
        )

    return vector


# ============================================================
# PROCESS DOCUMENT
# ============================================================

def process_document(
    collection,
    ollama,
    document
):

    text = document.get(
        "cleaned_text"
    )

    if not text:

        return False


    vector = create_embedding(
        ollama,
        text
    )


    result = collection.update_one(

        {
            "_id": document["_id"],

            "embedded": {
                "$ne": True
            }
        },

        {
            "$set": {

                "embedding": vector,

                "embedding_model":
                    EMBEDDING_MODEL,

                "embedding_dimension":
                    len(vector),

                "embedded": True,

                "embedded_at":
                    time.time(),

                # Si le document était déjà présent
                # et ré-embeddé, Qdrant doit le retraiter.
                "qdrant_stored": False
            }
        }
    )


    return result.modified_count == 1


# ============================================================
# MAIN
# ============================================================

def run():

    mongo, collection = connect_mongodb()

    ollama = connect_ollama()

    total_embedded = 0

    total_errors = 0


    print(
        "==========================================",
        flush=True
    )

    print(
        "       BLUESKY EMBEDDING SERVICE",
        flush=True
    )

    print(
        "==========================================",
        flush=True
    )

    print(
        f"[EMBEDDING] Model: "
        f"{EMBEDDING_MODEL}",
        flush=True
    )

    print(
        "[EMBEDDING] Running...",
        flush=True
    )


    while True:

        try:

            documents = collection.find(

                {
                    "preprocessed": True,

                    "accepted": True,

                    "cleaned_text": {
                        "$exists": True,
                        "$ne": ""
                    },

                    "embedded": {
                        "$ne": True
                    }
                },

                {
                    "cleaned_text": 1
                },

                batch_size=32
            )


            found = False


            for document in documents:

                found = True

                try:

                    text = document.get(
                        "cleaned_text",
                        ""
                    )

                    success = process_document(

                        collection,

                        ollama,

                        document
                    )


                    if success:

                        total_embedded += 1

                        print(

                            f"[EMBEDDING] "
                            f"#{total_embedded} | "
                            f"dimension OK | "
                            f"{text[:100]}",

                            flush=True
                        )


                except Exception as e:

                    total_errors += 1

                    print(

                        f"[EMBEDDING] "
                        f"Error: {e}",

                        flush=True
                    )


            if not found:

                time.sleep(
                    POLL_INTERVAL
                )


        except Exception as e:

            total_errors += 1

            print(

                f"[EMBEDDING] "
                f"Service error: {e}",

                flush=True
            )

            time.sleep(5)


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    run()