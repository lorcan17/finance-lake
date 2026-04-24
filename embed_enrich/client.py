import os
from collections.abc import Sequence

from openai import OpenAI

MODEL = "text-embedding-3-small"
DIMENSIONS = 1536


class EmbeddingClient:
    def __init__(self, api_key: str | None = None):
        key = api_key or os.environ.get("OPENAI_API_KEY")
        if not key:
            raise RuntimeError("OPENAI_API_KEY not set")
        self._client = OpenAI(api_key=key)

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        if not texts:
            return []
        resp = self._client.embeddings.create(model=MODEL, input=list(texts))
        return [d.embedding for d in resp.data]
