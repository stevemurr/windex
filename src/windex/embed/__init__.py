from windex.config import Settings
from windex.embed.base import Embedder


def build_embedder(settings: Settings) -> Embedder:
    if settings.embed_backend == "st":
        from windex.embed.st import SentenceTransformersEmbedder

        return SentenceTransformersEmbedder(settings.embed_model, settings.embed_dim)

    from windex.embed.http import HttpEmbedder

    style = "tei" if settings.embed_backend == "http-tei" else "openai"
    return HttpEmbedder(
        endpoint=settings.embed_endpoint,
        model_id=settings.embed_model,
        dim=settings.embed_dim,
        style=style,
        api_key=settings.embed_api_key,
    )
