"""Auto-embedding support for Redis OM vector fields (U1/U2).

A model field declares *how* it should be vectorized::

    class Doc(JsonModel, index=True):
        body: str = Field(full_text_search=True)
        embedding: list[float] = Field(
            vector_options=VectorFieldOptions.flat(...),
            vectorizer="openai:text-embedding-3-small",  # or a BaseVectorizer
            source="body",
        )

``save()`` then embeds the source text automatically whenever the vector
field is empty, and :func:`text_knn` builds KNN expressions from raw query
text.

This module is deliberately import-safe: it imports neither redisvl nor
``aredis_om.model`` at module level, so ``ModelMeta`` can import it while
``aredis_om.model.model`` is still initializing. RedisVL is imported lazily
on first use — models without vectorizers never trigger it.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:  # pragma: no cover
    from aredis_om.model.model import KNNExpression, RedisModel


@dataclasses.dataclass
class EmbeddingSpec:
    """Plain-data description of one auto-embedded field.

    Instances are created by :func:`build_embedding_specs` at class-creation
    time (via ``ModelMeta``) and stored on ``_meta.embedding_fields``. The
    vectorizer itself stays *unresolved* (a string spec or the user's
    instance) until first use, so defining a model never imports redisvl.
    """

    field_name: str
    source_field: Optional[str]
    vectorizer_ref: Any  # str spec like "openai:model-name" or a BaseVectorizer
    dimension: Optional[int] = None
    dtype: Any = None  # VectorFieldOptions.TYPE
    # Resolved lazily on first embed; per-model-class cache lives on _meta.
    _vectorizer: Any = dataclasses.field(default=None, repr=False)

    def resolve(self, model_cls: Optional[type] = None) -> Any:
        """Return the vectorizer instance, constructing it on first use.

        String specs (``"openai:text-embedding-3-small"``) are mapped through
        :data:`VECTORIZER_PROVIDERS`. When ``Meta.embedding_cache`` is enabled
        on the model, a Redis-backed :class:`redisvl.extensions.cache.embeddings.EmbeddingsCache`
        is attached to the resolved vectorizer (U2) — for both string- and
        instance-provided vectorizers (an existing ``cache`` on a shared
        instance is respected and left untouched).
        """
        if self._vectorizer is not None:
            return self._vectorizer
        self._vectorizer = _resolve_vectorizer_ref(self.vectorizer_ref)
        _maybe_attach_embedding_cache(self._vectorizer, model_cls)
        return self._vectorizer


# Provider registry for string specs: prefix -> provider key used by
# ``_resolve_vectorizer_ref``. Kept intentionally small; users needing other
# providers (Ollama, Voyage, custom callables) pass a vectorizer instance —
# or wrap a function with ``redisvl.utils.vectorize.CustomTextVectorizer``.
VECTORIZER_PROVIDERS: Dict[str, str] = {
    "openai": "OpenAITextVectorizer",
    "azure_openai": "AzureOpenAITextVectorizer",
    "cohere": "CohereTextVectorizer",
    "vertexai": "VertexAITextVectorizer",
    "bedrock": "BedrockTextVectorizer",
    "mistral": "MistralAITextVectorizer",
    "gemini": "GoogleGenAIVectorizer",
    "hf": "HFTextVectorizer",
}


class EmbeddingError(Exception):
    """Raised when auto-embedding cannot proceed (bad spec, provider error)."""


def _lazy_import_message() -> str:
    return (
        "Auto-embedding requires the 'redisvl' package. "
        "Install it with: pip install 'pyredis-om[redisvl]'"
    )


def _resolve_vectorizer_ref(ref: Any) -> Any:
    from redisvl.utils.vectorize import (
        AzureOpenAITextVectorizer,
        BedrockTextVectorizer,
        CohereTextVectorizer,
        GoogleGenAIVectorizer,
        HFTextVectorizer,
        MistralAITextVectorizer,
        OpenAITextVectorizer,
        VertexAITextVectorizer,
    )

    provider_classes = {
        "openai": OpenAITextVectorizer,
        "azure_openai": AzureOpenAITextVectorizer,
        "cohere": CohereTextVectorizer,
        "vertexai": VertexAITextVectorizer,
        "bedrock": BedrockTextVectorizer,
        "mistral": MistralAITextVectorizer,
        "gemini": GoogleGenAIVectorizer,
        "hf": HFTextVectorizer,
    }

    if not isinstance(ref, str):
        # A user-supplied vectorizer instance — accept duck-typed.
        if not (hasattr(ref, "embed") and hasattr(ref, "aembed")):
            raise EmbeddingError(
                "vectorizer= must be a string spec like "
                "'openai:text-embedding-3-small' or a redisvl BaseVectorizer "
                f"instance; got {type(ref).__name__}"
            )
        return ref

    provider, sep, model = ref.partition(":")
    if not sep or not model:
        raise EmbeddingError(
            f"Invalid vectorizer spec {ref!r}. Expected '<provider>:<model>', "
            f"e.g. 'openai:text-embedding-3-small'. Supported providers: "
            f"{', '.join(sorted(VECTORIZER_PROVIDERS))}."
        )
    provider = provider.strip().lower()
    cls = provider_classes.get(provider)
    if cls is None:
        raise EmbeddingError(
            f"Unknown vectorizer provider {provider!r}. Supported: "
            f"{', '.join(sorted(VECTORIZER_PROVIDERS))}. Pass a BaseVectorizer "
            "instance for anything else."
        )
    return cls(model=model)


def _maybe_attach_embedding_cache(vectorizer: Any, model_cls: Optional[type]) -> None:
    """Attach an EmbeddingsCache (U2) when ``Meta.embedding_cache`` is set.

    ``True`` means "auto": a cache named from the model's key prefix, wired
    to the model's database connection. An ``EmbeddingsCache`` instance is
    attached as-is. A vectorizer that already carries a cache (e.g. shared
    across models) is left untouched.
    """
    meta = getattr(model_cls, "_meta", None) if model_cls is not None else None
    setting = getattr(meta, "embedding_cache", None) if meta is not None else None
    if setting is None or setting is False:
        return
    if getattr(vectorizer, "cache", None) is not None:
        return
    if setting is True:
        from aredis_om.ai.cache import get_embedding_cache

        vectorizer.cache = get_embedding_cache(model_cls)
    else:
        vectorizer.cache = setting


def build_embedding_specs(model_cls: type) -> Dict[str, EmbeddingSpec]:
    """Collect ``Field(vectorizer=...)`` declarations into specs.

    Called from ``ModelMeta.__new__`` after OM field metadata has been applied
    to the class's fields. Validates:

    - the target field has ``vector_options`` (it must be a vector field);
    - ``source`` names an existing ``str`` field on the model (or is ``None``,
      meaning "manual embedding only" — ``save()`` won't auto-embed, but
      :func:`text_knn` still works);
    - the vectorizer ref is a string spec or a vectorizer-like instance.
    """
    specs: Dict[str, EmbeddingSpec] = {}
    for name, field in model_cls.model_fields.items():
        ref = getattr(field, "vectorizer", None)
        if ref is None:
            continue
        opts = getattr(field, "vector_options", None)
        if opts is None:
            from aredis_om.model.model import RedisModelError

            raise RedisModelError(
                f"Field {name!r} declares vectorizer= but is not a vector "
                "field: add vector_options=VectorFieldOptions.flat/hnsw/svs(...)."
            )
        source = getattr(field, "source", None)
        if source is not None:
            source_field = model_cls.model_fields.get(source)
            if source_field is None:
                from aredis_om.model.model import RedisModelError

                raise RedisModelError(
                    f"Field {name!r} declares source={source!r}, but no such "
                    f"field exists on {model_cls.__name__}."
                )
            # The source must serialize to text; require str at the top level.
            annotation = getattr(source_field, "annotation", None)
            if annotation is not str:
                from aredis_om.model.model import RedisModelError

                raise RedisModelError(
                    f"Field {name!r} source={source!r} must be a plain str "
                    f"field (got annotation {annotation!r})."
                )
        if not isinstance(ref, str) and not (
            hasattr(ref, "embed") and hasattr(ref, "aembed")
        ):
            from aredis_om.model.model import RedisModelError

            raise RedisModelError(
                f"Field {name!r}: vectorizer= must be '<provider>:<model>' or "
                "a redisvl BaseVectorizer instance."
            )
        specs[name] = EmbeddingSpec(
            field_name=name,
            source_field=source,
            vectorizer_ref=ref,
            dimension=getattr(opts, "dimension", None),
            dtype=getattr(opts, "type", None),
        )
    return specs


def _check_dimension(spec: EmbeddingSpec, vector: Any) -> None:
    if spec.dimension is not None and len(vector) != spec.dimension:
        raise EmbeddingError(
            f"Vectorizer returned {len(vector)} dimensions for field "
            f"{spec.field_name!r}, but vector_options declares "
            f"{spec.dimension}. Check the provider model's dimensionality."
        )


async def embed_text(spec: EmbeddingSpec, text: str, model_cls: Optional[type] = None) -> Any:
    """Embed one text with the spec's vectorizer (dimension-checked)."""
    try:
        vectorizer = spec.resolve(model_cls)
    except ImportError as e:
        raise ImportError(_lazy_import_message()) from e
    try:
        vector = await vectorizer.aembed(text)
    except EmbeddingError:
        raise
    except Exception as e:
        raise EmbeddingError(
            f"Embedding failed for field {spec.field_name!r}: {e}"
        ) from e
    _check_dimension(spec, vector)
    return vector


async def embed_model_fields(instance: Any) -> None:
    """Fill empty vector fields on a model instance from their sources.

    Called by ``save()`` (both HashModel and JsonModel) before serialization.
    Fields the user populated are left untouched; so are specs whose source
    field is empty/None.
    """
    model_cls = type(instance)
    specs = getattr(getattr(model_cls, "_meta", None), "embedding_fields", None)
    if not specs:
        return
    for spec in specs.values():
        current = getattr(instance, spec.field_name, None)
        if current:
            continue
        if spec.source_field is None:
            continue
        text = getattr(instance, spec.source_field, None)
        if not text:
            continue
        vector = await embed_text(spec, text, model_cls)
        setattr(instance, spec.field_name, vector)


async def text_knn(
    model_cls: type,
    text: str,
    field_name: Optional[str] = None,
    k: int = 10,
    score_field: Any = None,
) -> "KNNExpression":
    """Build a KNN expression from raw query text (auto-embedded).

    Example::

        knn = await text_knn(Doc, "running shoes", field_name="embedding", k=5)
        results = await Doc.find(knn=knn).all()

    Args:
        model_cls: The OM model class.
        text: Raw query text — embedded with the field's vectorizer.
        field_name: Vector field name. Optional when the model has exactly
            one auto-embedding field.
        k: Number of nearest neighbors.
        score_field: Optional score-field override (same as ``KNNExpression``).

    Raises:
        ValueError: If the model has no auto-embedding fields, or
            ``field_name`` is ambiguous/unknown.
    """
    from aredis_om.model.model import KNNExpression, _pack_vector

    specs = getattr(getattr(model_cls, "_meta", None), "embedding_fields", None)
    if not specs:
        raise ValueError(
            f"{model_cls.__name__} has no Field(vectorizer=...) declarations; "
            "pass a packed reference_vector to KNNExpression instead."
        )
    if field_name is None:
        if len(specs) != 1:
            raise ValueError(
                f"{model_cls.__name__} has multiple auto-embedding fields "
                f"({', '.join(sorted(specs))}); pass field_name=."
            )
        field_name = next(iter(specs))
    spec = specs.get(field_name)
    if spec is None:
        raise ValueError(
            f"{field_name!r} is not an auto-embedding field on "
            f"{model_cls.__name__} (fields: {', '.join(sorted(specs))})."
        )

    vector = await embed_text(spec, text, model_cls)
    vector_field = getattr(model_cls, field_name)
    field_info = model_cls.model_fields[field_name]
    opts = getattr(field_info, "vector_options", None)
    reference = _pack_vector(vector, opts.type, opts.dimension)
    return KNNExpression(
        k=k,
        vector_field=vector_field,
        reference_vector=reference,
        score_field=score_field,
    )
