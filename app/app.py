from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app.rag import ask_rag


# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(
    title="Bluesky RAG API",
    description="API RAG utilisant Qdrant et Ollama",
    version="1.0.0"
)


# ============================================================
# CORS
# ============================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


# ============================================================
# MODELS
# ============================================================

class QuestionRequest(BaseModel):
    question: str
    top_k: int = 5


# ============================================================
# ROOT
# ============================================================

@app.get("/")
def root():

    return {
        "status": "ok",
        "service": "Bluesky RAG API"
    }


# ============================================================
# HEALTH
# ============================================================

@app.get("/health")
def health():

    return {
        "status": "healthy"
    }


# ============================================================
# ASK RAG
# ============================================================

@app.post("/ask")
def ask(request: QuestionRequest):

    question = request.question.strip()

    if not question:

        raise HTTPException(
            status_code=400,
            detail="La question ne peut pas être vide."
        )

    try:

        result = ask_rag(
            question=question,
            top_k=request.top_k
        )

        return result

    except Exception as e:

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )
