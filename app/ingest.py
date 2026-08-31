import os
import re
import time

from pymongo import MongoClient
from langdetect import detect, DetectorFactory


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

MIN_TEXT_LENGTH = int(
    os.getenv(
        "MIN_TEXT_LENGTH",
        "10"
    )
)

POLL_INTERVAL = float(
    os.getenv(
        "INGEST_POLL_INTERVAL",
        "1"
    )
)


# ============================================================
# LANGUAGE DETECTOR
# ============================================================

DetectorFactory.seed = 0


# ============================================================
# MONGODB
# ============================================================

print(
    "[INGEST] Connecting to MongoDB...",
    flush=True
)

mongo_client = MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=5000
)

mongo_client.admin.command("ping")

db = mongo_client[
    MONGO_DATABASE
]

collection = db[
    MONGO_COLLECTION
]

print(
    "[INGEST] MongoDB connected",
    flush=True
)


# ============================================================
# INDEX
# ============================================================

# L'index permet de rechercher efficacement
# les documents qui n'ont pas encore été traités.

collection.create_index(
    [
        ("preprocessed", 1),
        ("ingested_at", 1)
    ]
)


# ============================================================
# URL PATTERN
# ============================================================

URL_PATTERN = re.compile(
    r"https?://\S+|www\.\S+",
    re.IGNORECASE
)


# ============================================================
# REMOVE URLS
# ============================================================

def remove_urls(text):

    return URL_PATTERN.sub(
        " ",
        text
    )


# ============================================================
# CLEAN SPECIAL CHARACTERS
# ============================================================

def clean_special_characters(text):

    # On garde les caractères utiles pour
    # un texte anglais.

    text = re.sub(
        r"[^A-Za-z0-9\s.,!?;:'\"()\-_$%&+#@]",
        " ",
        text
    )

    # Supprimer les espaces multiples

    text = re.sub(
        r"\s+",
        " ",
        text
    )

    return text.strip()


# ============================================================
# LANGUAGE DETECTION
# ============================================================

def is_english(text):

    try:

        language = detect(
            text
        )

        return language == "en"

    except Exception:

        return False


# ============================================================
# PREPROCESS
# ============================================================

def preprocess(text):

    # --------------------------------------------------------
    # Vérification du type
    # --------------------------------------------------------

    if not isinstance(
        text,
        str
    ):
        return None


    # --------------------------------------------------------
    # Supprimer espaces au début/fin
    # --------------------------------------------------------

    text = text.strip()


    if not text:

        return None


    # --------------------------------------------------------
    # Supprimer URLs
    # --------------------------------------------------------

    text = remove_urls(
        text
    )


    # --------------------------------------------------------
    # Nettoyer caractères spéciaux
    # --------------------------------------------------------

    text = clean_special_characters(
        text
    )


    # --------------------------------------------------------
    # Vérifier longueur
    # --------------------------------------------------------

    if len(text) < MIN_TEXT_LENGTH:

        return None


    # --------------------------------------------------------
    # Vérifier langue
    # --------------------------------------------------------

    if not is_english(
        text
    ):

        return None


    return text


# ============================================================
# PROCESS ONE DOCUMENT
# ============================================================

def process_document(document):

    original_text = document.get(
        "text"
    )


    cleaned_text = preprocess(
        original_text
    )


    # ========================================================
    # REJECT
    # ========================================================

    if cleaned_text is None:

        collection.update_one(

            {
                "_id": document["_id"]
            },

            {
                "$set": {
                    "preprocessed": True,
                    "accepted": False
                }
            }
        )

        return False


    # ========================================================
    # ACCEPT
    # ========================================================

    collection.update_one(

        {
            "_id": document["_id"]
        },

        {
            "$set": {

                "cleaned_text": cleaned_text,

                "language": "en",

                "preprocessed": True,

                "accepted": True,

                "processed_at": time.time()
            }
        }
    )


    print(
        f"[INGEST] ACCEPTED | "
        f"{cleaned_text[:120]}",
        flush=True
    )


    return True


# ============================================================
# MAIN PROCESSING LOOP
# ============================================================

def run():

    total_processed = 0
    total_accepted = 0
    total_rejected = 0


    print(
        "==========================================",
        flush=True
    )

    print(
        "       BLUESKY INGEST / PREPROCESSING",
        flush=True
    )

    print(
        "==========================================",
        flush=True
    )

    print(
        f"[INGEST] Database   : {MONGO_DATABASE}",
        flush=True
    )

    print(
        f"[INGEST] Collection : {MONGO_COLLECTION}",
        flush=True
    )

    print(
        f"[INGEST] Min length : {MIN_TEXT_LENGTH}",
        flush=True
    )

    print(
        "[INGEST] Running...",
        flush=True
    )


    while True:

        try:

            # =================================================
            # Chercher les posts non traités
            # =================================================

            documents = collection.find(

                {
                    "$or": [

                        {
                            "preprocessed": {
                                "$exists": False
                            }
                        },

                        {
                            "preprocessed": False
                        }

                    ]
                },

                {
                    "text": 1,
                    "uri": 1,
                    "author_did": 1,
                    "ingested_at": 1
                },

                batch_size=100
            )


            found = False


            for document in documents:

                found = True

                total_processed += 1


                try:

                    accepted = process_document(
                        document
                    )


                    if accepted:

                        total_accepted += 1

                    else:

                        total_rejected += 1


                except Exception as e:

                    print(
                        f"[INGEST] Document error: {e}",
                        flush=True
                    )


            # =================================================
            # Statistiques
            # =================================================

            if found:

                print(
                    f"[INGEST] "
                    f"processed={total_processed} | "
                    f"accepted={total_accepted} | "
                    f"rejected={total_rejected}",
                    flush=True
                )


            # =================================================
            # Attendre nouveaux posts
            # =================================================

            time.sleep(
                POLL_INTERVAL
            )


        except Exception as e:

            print(
                f"[INGEST] Error: {e}",
                flush=True
            )

            print(
                "[INGEST] Retrying in 5 seconds...",
                flush=True
            )

            time.sleep(
                5
            )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    run()

