"""Vector compression advisor (U8).

Delegates to redisvl's ``CompressionAdvisor`` to recommend an SVS-VAMANA
compression strategy (LVQ / LeanVec) for a model's vector field, plus an
estimated memory saving.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

from aredis_om.ai._connection import _lazy_import_message

if TYPE_CHECKING:  # pragma: no cover
    from aredis_om.model.model import RedisModel


def _get_vector_options(model_cls: type, field_name: str) -> Any:
    field_info = model_cls.model_fields.get(field_name)
    if field_info is None:
        raise ValueError(
            f"Field {field_name!r} does not exist on {model_cls.__name__}."
        )
    opts = getattr(field_info, "vector_options", None)
    if opts is None:
        raise ValueError(
            f"Field {field_name!r} on {model_cls.__name__} has no "
            "vector_options — compression applies to vector fields only."
        )
    return opts


def recommend_compression(
    model_cls: type,
    field_name: str,
    priority: str = "balanced",
) -> "Tuple[Any, Dict[str, Any]]":
    """Recommend an SVS-VAMANA compression strategy for a vector field.

    Thin wrapper over redisvl's ``CompressionAdvisor.recommend`` — the
    recommendation is computed from the field's dimensionality and datatype
    (redisvl's advisor is heuristic; no data sample is required).

    Args:
        model_cls: OM model class declaring the vector field.
        field_name: Name of the vector field.
        priority: ``"speed"``, ``"memory"``, or ``"balanced"`` (default).

    Returns:
        ``(svs_config, summary)`` — the redisvl ``SVSConfig`` (ready to feed
        ``VectorFieldOptions.svs(...)``) and a plain-dict summary with the
        recommended ``compression`` and estimated memory ``saving`` fraction.

    Example::

        svs_cfg, summary = recommend_compression(Doc, "embedding", "memory")
        print(summary["compression"], summary["memory_saving_fraction"])
    """
    try:
        from redisvl.utils.compression import CompressionAdvisor
    except ImportError as e:  # pragma: no cover
        raise ImportError(_lazy_import_message()) from e

    opts = _get_vector_options(model_cls, field_name)
    datatype = getattr(getattr(opts, "type", None), "value", None)
    advisor = CompressionAdvisor()
    svs_config = advisor.recommend(
        dims=opts.dimension, priority=priority, datatype=datatype
    )
    compression = getattr(svs_config, "compression", None)
    reduce = getattr(svs_config, "reduce", None)
    saving = advisor.estimate_memory_savings(
        compression=compression, dims=opts.dimension, reduce=reduce
    )
    summary = {
        "compression": compression,
        "reduce": reduce,
        "memory_saving_fraction": saving,
        "priority": priority,
    }
    return svs_config, summary


def estimate_memory_savings(
    model_cls: type,
    field_name: str,
    compression: Optional[str] = None,
    reduce: Optional[int] = None,
) -> float:
    """Estimate the memory-saving fraction for a compression choice.

    Delegates to ``CompressionAdvisor.estimate_memory_savings``. When
    ``compression`` is omitted, the advisor's balanced recommendation for
    the field is used.
    """
    try:
        from redisvl.utils.compression import CompressionAdvisor
    except ImportError as e:  # pragma: no cover
        raise ImportError(_lazy_import_message()) from e

    opts = _get_vector_options(model_cls, field_name)
    advisor = CompressionAdvisor()
    if compression is None:
        datatype = getattr(getattr(opts, "type", None), "value", None)
        svs_config = advisor.recommend(
            dims=opts.dimension, priority="balanced", datatype=datatype
        )
        compression = getattr(svs_config, "compression", None)
        reduce = getattr(svs_config, "reduce", None)
    return advisor.estimate_memory_savings(
        compression=compression, dims=opts.dimension, reduce=reduce
    )
