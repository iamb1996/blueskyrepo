# ============================================================
# app/rag.py
# QDRANT + OLLAMA + RAG
# ============================================================

import os
import sys
import uuid

import requests

from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    PointStruct
)


# ============================================================
# CONFIGURATION
# ============================================================

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

OLLAMA_HOST = os.getenv(
    "OLLAMA_HOST",
    "http://ollama:11434"
)

EMBEDDING_MODEL = os.getenv(
    "EMBEDDING_MODEL",
    "nomic-embed-text"
)

LLM_MODEL = os.getenv(
    "LLM_MODEL",
    "qwen3:8b"
)

COLLECTION_NAME = os.getenv(
    "QDRANT_COLLECTION",
    "bluesky_posts"
)

VECTOR_SIZE = int(
    os.getenv(
        "VECTOR_SIZE",
        "768"
    )
)

TOP_K = int(
    os.getenv(
        "TOP_K",
        "5"
    )
)


# ============================================================
# QDRANT CLIENT
# ============================================================

def get_qdrant_client():
    """
    Connexion à Qdrant.
    """

    print(
        f"[QDRANT] Connexion à "
        f"{QDRANT_HOST}:{QDRANT_PORT}"
    )

    client = QdrantClient(
        host=QDRANT_HOST,
        port=QDRANT_PORT
    )

    return client


# ============================================================
# CREATE COLLECTION
# ============================================================

def create_collection(client):
    """
    Crée la collection Qdrant si elle n'existe pas.
    """

    collections = client.get_collections()

    exists = any(
        collection.name == COLLECTION_NAME
        for collection in collections.collections
    )

    if exists:

        print(
            f"[QDRANT] Collection "
            f"'{COLLECTION_NAME}' déjà existante"
        )

        return

    print(
        f"[QDRANT] Création de la collection "
        f"'{COLLECTION_NAME}'"
    )

    client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(
            size=VECTOR_SIZE,
            distance=Distance.COSINE
        )
    )

    print("[QDRANT] Collection créée")


# ============================================================
# OLLAMA EMBEDDING
# ============================================================

def create_embedding(text):
    """
    Génère un embedding avec Ollama.
    """

    if not text or not text.strip():
        return None

    url = f"{OLLAMA_HOST}/api/embeddings"

    payload = {
        "model": EMBEDDING_MODEL,
        "prompt": text
    }

    try:

        response = requests.post(
            url,
            json=payload,
            timeout=120
        )

        response.raise_for_status()

        data = response.json()

        embedding = data.get(
            "embedding"
        )

        if not embedding:

            print(
                "[EMBEDDING] Aucun embedding retourné"
            )

            return None

        return embedding

    except requests.exceptions.RequestException as e:

        print(
            f"[EMBEDDING] Erreur Ollama : {e}"
        )

        return None


# ============================================================
# INSERT DOCUMENT
# ============================================================

def insert_document(
    client,
    text,
    payload=None
):
    """
    Crée l'embedding du texte et l'insère dans Qdrant.
    """

    embedding = create_embedding(
        text
    )

    if embedding is None:

        return False

    point_id = str(
        uuid.uuid4()
    )

    if payload is None:

        payload = {}

    payload["text"] = text

    point = PointStruct(
        id=point_id,
        vector=embedding,
        payload=payload
    )

    client.upsert(
        collection_name=COLLECTION_NAME,
        points=[point]
    )

    print(
        f"[QDRANT] Document inséré : "
        f"{point_id}"
    )

    return True


# ============================================================
# VECTOR SEARCH
# ============================================================

def search_documents(
    client,
    query,
    top_k=TOP_K
):
    """
    Recherche les documents les plus proches
    de la question.
    """

    print(
        f"[RAG] Question : {query}"
    )

    query_embedding = create_embedding(
        query
    )

    if query_embedding is None:

        return []

    try:

        results = client.query_points(
            collection_name=COLLECTION_NAME,
            query=query_embedding,
            limit=top_k,
            with_payload=True
        )

        documents = []

        for result in results.points:

            payload = (
                result.payload
                or {}
            )

            text = payload.get(
                "text",
                ""
            )

            documents.append(
                {
                    "text": text,
                    "score": result.score,
                    "payload": payload
                }
            )

        print(
            f"[QDRANT] "
            f"{len(documents)} documents trouvés"
        )

        return documents

    except Exception as e:

        print(
            f"[QDRANT] Erreur recherche : {e}"
        )

        return []


# ============================================================
# BUILD CONTEXT
# ============================================================

def build_context(documents):
    """
    Construit le contexte envoyé au LLM.
    """

    if not documents:

        return (
            "Aucun document pertinent trouvé."
        )

    context_parts = []

    for i, document in enumerate(
        documents,
        1
    ):

        text = document.get(
            "text",
            ""
        )

        score = document.get(
            "score",
            0
        )

        context_parts.append(
            f"[Document {i} | "
            f"score={score:.4f}]\n"
            f"{text}"
        )

    return "\n\n".join(
        context_parts
    )


# ============================================================
# QWEN GENERATION
# ============================================================

def generate_answer(
    question,
    context
):
    """
    Envoie le contexte et la question à Qwen via Ollama.
    """

    url = (
        f"{OLLAMA_HOST}"
        f"/api/generate"
    )

    prompt = f"""
Tu es un assistant RAG spécialisé dans l'analyse
de publications provenant de Bluesky.

Tu dois répondre uniquement à partir du contexte fourni.

Si le contexte ne contient pas suffisamment
d'informations pour répondre, indique clairement
que l'information n'est pas disponible.

Ne fabrique pas d'informations.

CONTEXTE :
--------------------
{context}
--------------------

QUESTION :
{question}

REPONSE :
"""

    payload = {
        "model": LLM_MODEL,
        "prompt": prompt,
        "stream": False
    }

    try:

        print(
            f"[LLM] Génération avec "
            f"{LLM_MODEL}"
        )

        response = requests.post(
            url,
            json=payload,
            timeout=300
        )

        response.raise_for_status()

        data = response.json()

        answer = data.get(
            "response",
            ""
        )

        return answer.strip()

    except requests.exceptions.RequestException as e:

        print(
            f"[LLM] Erreur Ollama : {e}"
        )

        return None


# ============================================================
# FASTAPI RAG FUNCTION
# ============================================================

def ask_rag(
    question,
    top_k=TOP_K
):
    """
    Fonction principale utilisée par FastAPI.

    Retourne un dictionnaire Python automatiquement
    converti en JSON par FastAPI.
    """

    # --------------------------------------------------------
    # VALIDATION QUESTION
    # --------------------------------------------------------

    if not question:

        return {
            "question": "",
            "answer": "La question est vide.",
            "sources": []
        }

    question = question.strip()

    if not question:

        return {
            "question": "",
            "answer": "La question est vide.",
            "sources": []
        }

    print("\n" + "=" * 60)
    print("[RAG] NOUVELLE QUESTION")
    print("=" * 60)

    print(
        f"[QUESTION] {question}"
    )

    # --------------------------------------------------------
    # QDRANT
    # --------------------------------------------------------

    try:

        client = get_qdrant_client()

        create_collection(
            client
        )

    except Exception as e:

        print(
            f"[QDRANT] Erreur connexion : {e}"
        )

        return {
            "question": question,
            "answer": None,
            "sources": [],
            "error": (
                f"Erreur Qdrant : {str(e)}"
            )
        }

    # --------------------------------------------------------
    # SEARCH
    # --------------------------------------------------------

    documents = search_documents(
        client=client,
        query=question,
        top_k=top_k
    )

    # --------------------------------------------------------
    # NO DOCUMENT
    # --------------------------------------------------------

    if not documents:

        return {
            "question": question,
            "answer": (
                "Je n'ai trouvé aucun document "
                "pertinent dans la base."
            ),
            "sources": []
        }

    # --------------------------------------------------------
    # CONTEXT
    # --------------------------------------------------------

    context = build_context(
        documents
    )

    print("\n[RAG] CONTEXTE :")
    print("-" * 60)
    print(context)
    print("-" * 60)

    # --------------------------------------------------------
    # QWEN GENERATION
    # --------------------------------------------------------

    answer = generate_answer(
        question=question,
        context=context
    )

    # --------------------------------------------------------
    # GENERATION ERROR
    # --------------------------------------------------------

    if answer is None:

        return {
            "question": question,
            "answer": None,
            "sources": [],
            "error": (
                "Erreur lors de la génération "
                "avec Ollama."
            )
        }

    # --------------------------------------------------------
    # BUILD SOURCES
    # --------------------------------------------------------

    sources = []

    for document in documents:

        sources.append(
            {
                "text": document.get(
                    "text",
                    ""
                ),

                "score": document.get(
                    "score",
                    0
                ),

                "metadata": document.get(
                    "payload",
                    {}
                )
            }
        )

    # --------------------------------------------------------
    # FINAL RESPONSE
    # --------------------------------------------------------

    result = {
        "question": question,
        "answer": answer,
        "sources": sources
    }

    print("\n[RAG] RÉPONSE :")
    print("=" * 60)
    print(answer)
    print("=" * 60)

    return result


# ============================================================
# COMPATIBILITY FUNCTION
# ============================================================

def rag(
    question,
    top_k=TOP_K
):
    """
    Ancienne fonction compatible avec les tests précédents.

    Retourne uniquement la réponse texte.
    """

    result = ask_rag(
        question=question,
        top_k=top_k
    )

    return result.get(
        "answer"
    )


# ============================================================
# OPTIONAL TERMINAL TEST
# ============================================================

def main():
    """
    Test manuel optionnel du RAG.
    """

    print("=" * 60)
    print("RAG - QDRANT + OLLAMA")
    print("=" * 60)

    # --------------------------------------------------------
    # QDRANT TEST
    # --------------------------------------------------------

    try:

        client = get_qdrant_client()

        print(
            "[QDRANT] Connexion OK"
        )

        create_collection(
            client
        )

    except Exception as e:

        print(
            f"[QDRANT] "
            f"Connexion impossible : {e}"
        )

        sys.exit(1)

    # --------------------------------------------------------
    # EMBEDDING TEST
    # --------------------------------------------------------

    print(
        "\n[EMBEDDING] Test de "
        f"{EMBEDDING_MODEL}"
    )

    test_embedding = create_embedding(
        "test embedding"
    )

    if test_embedding is None:

        print(
            "[EMBEDDING] Échec"
        )

        sys.exit(1)

    print(
        "[EMBEDDING] OK"
    )

    print(
        f"[EMBEDDING] Dimension : "
        f"{len(test_embedding)}"
    )

    # --------------------------------------------------------
    # QUESTION
    # --------------------------------------------------------

    if len(sys.argv) > 1:

        question = " ".join(
            sys.argv[1:]
        )

    else:

        question = input(
            "\nPose ta question : "
        )

    # --------------------------------------------------------
    # RAG
    # --------------------------------------------------------

    result = ask_rag(
        question
    )

    print("\nRESULTAT JSON :")
    print(result)


# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":

    main()
