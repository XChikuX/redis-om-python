# mypy: disable-error-code="assignment,arg-type,union-attr,no-redef"

import abc
import asyncio
import base64
import collections.abc
import dataclasses
import datetime
import decimal
import hashlib
import hmac
import json
import logging
import operator
import types
from copy import copy
from enum import Enum
from functools import reduce
from typing import (
    AbstractSet,
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from typing import get_args as typing_get_args
from typing import no_type_check

from more_itertools import ichunked
from pydantic import ConfigDict, model_validator
from redis import ResponseError
from redis.commands.json.path import Path
from typing_extensions import Annotated, Protocol, TypeGuard, get_args, get_origin
from ulid import ULID

from .. import redis
from .._compat import BaseModel
from .._compat import FieldInfo as PydanticFieldInfo
from .._compat import ModelField, ModelMetaclass, NoArgAnyCallable
from .._compat import PydanticUndefined as Undefined
from .._compat import Representation, UndefinedType
from ..checks import has_redis_json, has_redisearch
from ..connections import get_redis_connection, protocol_version
from ..util import ASYNC_MODE, has_numeric_inner_type, is_numeric_type
from .encoders import jsonable_encoder
from .render_tree import render_tree
from .resp3_shim import (
    _decode_dict_keys,
    extract_key_from_row,
    is_resp3_search_response,
    split_cursor_response,
    split_search_response,
)
from .token_escaper import TokenEscaper
from .types import Coordinates, GeoFilter

model_registry: dict[type, type] = {}
_T = TypeVar("_T")
Model = TypeVar("Model", bound="RedisModel")
log = logging.getLogger(__name__)
escaper = TokenEscaper()
DatabaseConnection = Union[redis.Redis, redis.RedisCluster]
DatabaseProvider = Callable[[], DatabaseConnection]


def _decode_token_value(value: Union[str, bytes]) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "ignore")
    return value


def _urlsafe_b64decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(f"{value}{padding}".encode("ascii"))


def _urlsafe_b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _is_cluster_pipeline(db) -> bool:
    """Check if a database object is an async RedisCluster pipeline.

    ClusterPipeline commands must NOT be awaited—doing so consumes the
    response immediately instead of queuing the command for batch execution.
    Regular async Pipeline commands may be safely awaited (they return the
    pipeline instance for chaining while still queueing the command).
    """
    try:
        from redis.asyncio.cluster import ClusterPipeline

        if isinstance(db, ClusterPipeline):
            return True
    except ImportError:
        pass
    # Fallback: check class name in case the import path changes.
    return type(db).__name__ == "ClusterPipeline"


# For basic exact-match field types like an indexed string, we create a TAG
# field in the RediSearch index. TAG is designed for multi-value fields
# separated by a "separator" character. We're using the field for single values
# (multi-value TAGs will be exposed as a separate field type), and we use the
# pipe character (|) as the separator. There is no way to escape this character
# in hash fields or JSON objects, so if someone indexes a value that includes
# the pipe, we'll warn but allow, and then warn again if they try to query for
# values that contain this separator.
SINGLE_VALUE_TAG_FIELD_SEPARATOR = "|"


# This is the default field separator in RediSearch. We need it to determine if
# someone has accidentally passed in the field separator with string value of a
# multi-value field lookup, like a IN or NOT_IN.
DEFAULT_REDISEARCH_FIELD_SEPARATOR = ","

ERRORS_URL = "https://github.com/XChikuX/redis-om-python/blob/main/docs/errors.mdx"


def _is_union_type(annotation: Any) -> bool:
    """Return whether an annotation is typing.Union or PEP 604 syntax (e.g. str | None)."""
    origin = get_origin(annotation)
    return origin is Union or origin is types.UnionType


def _unwrap_type_annotation(annotation: Any) -> Any:
    """Unwrap Annotated and union annotations to the type used for schemas."""
    origin = get_origin(annotation)
    if origin is Annotated:
        args = get_args(annotation)
        if args:
            return _unwrap_type_annotation(args[0])
    if _is_union_type(annotation):
        args = [arg for arg in get_args(annotation) if arg is not types.NoneType]
        if args:
            return _unwrap_type_annotation(args[0])
    return annotation


def get_outer_type(field):
    if hasattr(field, "outer_type_"):
        return field.outer_type_
    annotation = _unwrap_type_annotation(field.annotation)
    origin = get_origin(annotation)
    if origin == Literal:
        return annotation
    if isinstance(annotation, type) or is_supported_container_type(annotation):
        return annotation
    if not hasattr(annotation, "__args__"):
        return None
    return annotation.__args__[0]


class RedisModelError(Exception):
    """Raised when a problem exists in the definition of a RedisModel."""


class QuerySyntaxError(Exception):
    """Raised when a query is constructed improperly."""


class NotFoundError(Exception):
    """Raised when a query found no results."""


class Operators(Enum):
    EQ = 1
    NE = 2
    LT = 3
    LE = 4
    GT = 5
    GE = 6
    OR = 7
    AND = 8
    NOT = 9
    IN = 10
    NOT_IN = 11
    LIKE = 12
    ALL = 13
    STARTSWITH = 14
    ENDSWITH = 15
    CONTAINS = 16
    TRUE = 17
    FALSE = 18

    def __str__(self):
        return str(self.name)


ExpressionOrModelField = Union[
    "Expression", "NegatedExpression", ModelField, PydanticFieldInfo
]


def embedded(cls):
    """
    Mark a model as embedded to avoid creating multiple indexes if the model is
    only ever used embedded within other models.
    """
    setattr(cls.Meta, "embedded", True)


def is_supported_container_type(typ: Optional[type]) -> bool:
    """Check if a type annotation is a supported container for indexing.

    Only ``list`` and ``tuple`` are supported. Sets are intentionally excluded
    because RediSearch TAG fields do not preserve insertion order, which is
    fundamental to set semantics. Additionally, sets would make query results
    non-deterministic since the ordering of matched set members is not guaranteed.
    """
    if typ == list or typ == tuple:
        return True
    unwrapped = get_origin(typ)
    return unwrapped == list or unwrapped == tuple


def validate_model_fields(model: Type["RedisModel"], field_values: Dict[str, Any]):
    for field_name in field_values:
        if "__" in field_name:
            obj = model
            for sub_field in field_name.split("__"):
                if not isinstance(obj, ModelMeta) and hasattr(obj, "field"):
                    annotation = getattr(obj, "field").annotation
                    # Unwrap Optional[X] (typing.Union[X, None] or PEP 604
                    # X | None) so that we can traverse into the inner model.
                    if _is_union_type(annotation):
                        annotation = next(
                            (
                                a
                                for a in typing_get_args(annotation)
                                if a is not type(None)
                            ),
                            annotation,
                        )
                    obj = annotation

                if not hasattr(obj, sub_field):
                    raise QuerySyntaxError(
                        f"The update path {field_name} contains a field that does not "
                        f"exist on {model.__name__}. The field is: {sub_field}"
                    )
                obj = getattr(obj, sub_field)
            return

        if field_name not in model.model_fields:
            raise QuerySyntaxError(
                f"The field {field_name} does not exist on the model {model.__name__}"
            )


def get_model_fields(model: Any) -> Mapping[str, Any]:
    """Return Pydantic v2 field mappings."""
    return getattr(model, "model_fields", {})


def has_model_field_mapping(model: Any) -> bool:
    """Check whether a model exposes Pydantic field mappings."""
    return hasattr(model, "model_fields")


# Internal key used to reuse the document-level type converters
# (``convert_timestamp_to_datetime`` / ``convert_base64_to_bytes``) when
# deserializing a single sub-value retrieved via ``JsonModel.get_value()``.
_SUB_VALUE_KEY = "__redis_om_sub_value__"


class _SubValueField:
    """Lightweight stand-in for a Pydantic field that only carries the
    annotation, so the existing dict-based type converters can be reused to
    deserialize an individual JSON sub-value."""

    __slots__ = ("annotation",)

    def __init__(self, annotation: Any) -> None:
        self.annotation = annotation


def is_model_field_instance(value: Any) -> TypeGuard[ModelField]:
    """Detect both legacy and compatibility ModelField-like objects."""
    return hasattr(value, "name") and hasattr(value, "field_info")


def validate_model_data(model: Any, values: Any) -> Any:
    """Validate model data with Pydantic v2."""
    return model.model_validate(values)


def restore_missing_pk(model: Any, values: Any, requested_pk: Any) -> Any:
    """Backfill a missing top-level pk from the Redis key used for loading."""
    if (
        not isinstance(values, dict)
        or requested_pk is None
        or getattr(getattr(model, "_meta", None), "embedded", False)
        or values.get("pk")
    ):
        return values
    values = dict(values)
    # RedisModel.pk is declared as Optional[str], so reload-time backfills
    # should normalize the requested key to the string form used by the model.
    values["pk"] = str(requested_pk)
    return values


def _is_embedded_json_model(cls: Any) -> bool:
    """Check if a class is an EmbeddedJsonModel subclass."""
    # Avoid circular import — check by name / MRO.
    for base in getattr(cls, "__mro__", []):
        if base.__name__ == "EmbeddedJsonModel":
            return True
    return False


def strip_null_embedded_pks(model: Any, values: Any) -> Any:
    """Recursively remove primary keys from embedded-model dump output."""
    if not isinstance(values, dict) or not has_model_field_mapping(model):
        return values

    cleaned = dict(values)
    for field_name, field in get_model_fields(model).items():
        if field_name not in cleaned:
            continue

        field_type = outer_type_or_annotation(field)
        value = cleaned[field_name]

        if is_supported_container_type(field_type):
            type_args = get_args(field_type)
            inner_type = type_args[0] if type_args else None
            if (
                isinstance(value, list)
                and isinstance(inner_type, type)
                and issubclass(inner_type, RedisModel)
            ):
                cleaned[field_name] = [
                    (
                        strip_null_embedded_pks(inner_type, item)
                        if isinstance(item, dict)
                        else item
                    )
                    for item in value
                ]
        elif (
            isinstance(field_type, type)
            and issubclass(field_type, RedisModel)
            and isinstance(value, dict)
        ):
            cleaned[field_name] = strip_null_embedded_pks(field_type, value)
            # EmbeddedJsonModel always strips pk.  Other embedded models
            # (e.g. embedded HashModel) only strip null/placeholder pk so
            # that user-set values (like composite keys) are preserved.
            if getattr(field_type, "_meta", None) and getattr(
                field_type._meta, "embedded", False
            ):
                if _is_embedded_json_model(field_type):
                    cleaned[field_name].pop("pk", None)
                elif cleaned[field_name].get("pk") is None:
                    cleaned[field_name].pop("pk", None)

    if getattr(model._meta, "embedded", False):
        if _is_embedded_json_model(model):
            cleaned.pop("pk", None)
        elif cleaned.get("pk") is None:
            cleaned.pop("pk", None)
    return cleaned


def decode_redis_value(
    obj: Union[List[bytes], Dict[bytes, bytes], bytes], encoding: str
) -> Union[List[str], Dict[str, str], str]:
    """Decode a binary-encoded Redis hash into the specified encoding."""
    if isinstance(obj, list):
        return [v.decode(encoding) for v in obj]
    if isinstance(obj, dict):
        return {
            key.decode(encoding): value.decode(encoding) for key, value in obj.items()
        }
    elif isinstance(obj, bytes):
        return obj.decode(encoding)


def remove_prefix(value: str, prefix: str) -> str:
    """Remove a prefix from a string."""
    return value.removeprefix(prefix)


class PipelineError(Exception):
    """A Redis pipeline error."""


def verify_pipeline_response(
    response: List[Union[bytes, str]], expected_responses: int = 0
):
    """Verify a Redis pipeline response has the expected number of results.

    This is intentionally minimal: it only checks the response count, not the
    content of individual responses. Per-command response validation is left to
    callers because pipeline responses for ``HSET``, ``JSON.SET``, and other
    commands differ in shape and are consumed by the model code that issues
    them. If you need stricter validation, add a hash- or JSON-specific verifier
    on top of this function rather than changing its signature.
    """
    actual_responses = len(response)
    if actual_responses != expected_responses:
        raise PipelineError(
            f"We expected {expected_responses}, but the Redis "
            f"pipeline returned {actual_responses} responses."
        )


@dataclasses.dataclass
class NegatedExpression:
    """A negated Expression object.

    For now, this is a separate dataclass from Expression that acts as a facade
    around an Expression, indicating to model code (specifically, code
    responsible for querying) to negate the logic in the wrapped Expression. A
    better design is probably possible, maybe at least an ExpressionProtocol?
    """

    expression: "Expression"

    def __invert__(self):
        return self.expression

    def __and__(self, other):
        return Expression(
            left=self, op=Operators.AND, right=other, parents=self.expression.parents
        )

    def __or__(self, other):
        return Expression(
            left=self, op=Operators.OR, right=other, parents=self.expression.parents
        )

    @property
    def left(self):
        return self.expression.left

    @property
    def right(self):
        return self.expression.right

    @property
    def op(self):
        return self.expression.op

    @property
    def name(self):
        if self.expression.op is Operators.EQ:
            return f"NOT {self.expression.name}"
        else:
            return f"{self.expression.name} NOT"

    @property
    def tree(self):
        return render_tree(self)


@dataclasses.dataclass
class Expression:
    op: Operators
    left: Optional[ExpressionOrModelField]
    right: Optional[ExpressionOrModelField]
    parents: List[Tuple[str, "RedisModel"]]

    def __invert__(self):
        return NegatedExpression(self)

    def __and__(self, other: ExpressionOrModelField):
        return Expression(
            left=self, op=Operators.AND, right=other, parents=self.parents
        )

    def __or__(self, other: ExpressionOrModelField):
        return Expression(left=self, op=Operators.OR, right=other, parents=self.parents)

    @property
    def name(self):
        return str(self.op)

    @property
    def tree(self):
        return render_tree(self)


@dataclasses.dataclass(init=False)
class KNNExpression:
    k: int
    vector_field: ModelField
    reference_vector: bytes
    _score_field: Optional[str]

    def __init__(
        self,
        k: int,
        vector_field: Any,
        reference_vector: bytes,
        score_field: Optional[Any] = None,
    ):
        self.k = k
        self.vector_field = (
            vector_field.field if hasattr(vector_field, "field") else vector_field
        )
        self.reference_vector = reference_vector
        if score_field is None:
            self._score_field = None
        elif hasattr(score_field, "field"):
            self._score_field = score_field.field.name
        elif hasattr(score_field, "name"):
            self._score_field = score_field.name
        else:
            self._score_field = str(score_field)

    def __str__(self):
        return f"KNN $K @{self.vector_field.name} $knn_ref_vector AS {self.score_field}"

    @property
    def query_params(self) -> Dict[str, Union[str, bytes]]:
        return {"K": str(self.k), "knn_ref_vector": self.reference_vector}

    @property
    def score_field(self) -> str:
        return self._score_field or f"__{self.vector_field.name}_score"


ExpressionOrNegated = Union[Expression, NegatedExpression]


class ExpressionProxy:
    def __init__(self, field: ModelField, parents: List[Tuple[str, "RedisModel"]]):
        self.field = field
        self.parents = parents.copy()

    def __eq__(self, other: Any) -> Expression:  # type: ignore[override]
        return Expression(
            left=self.field, op=Operators.EQ, right=other, parents=self.parents
        )

    def __ne__(self, other: Any) -> Expression:  # type: ignore[override]
        return Expression(
            left=self.field, op=Operators.NE, right=other, parents=self.parents
        )

    def __lt__(self, other: Any) -> Expression:
        return Expression(
            left=self.field, op=Operators.LT, right=other, parents=self.parents
        )

    def __le__(self, other: Any) -> Expression:
        return Expression(
            left=self.field, op=Operators.LE, right=other, parents=self.parents
        )

    def __gt__(self, other: Any) -> Expression:
        return Expression(
            left=self.field, op=Operators.GT, right=other, parents=self.parents
        )

    def __ge__(self, other: Any) -> Expression:
        return Expression(
            left=self.field, op=Operators.GE, right=other, parents=self.parents
        )

    def __mod__(self, other: Any) -> Expression:
        return Expression(
            left=self.field, op=Operators.LIKE, right=other, parents=self.parents
        )

    def __lshift__(self, other: Any) -> Expression:
        return Expression(
            left=self.field, op=Operators.IN, right=other, parents=self.parents
        )

    def __rshift__(self, other: Any) -> Expression:
        return Expression(
            left=self.field, op=Operators.NOT_IN, right=other, parents=self.parents
        )

    def startswith(self, other: Any) -> Expression:
        return Expression(
            left=self.field, op=Operators.STARTSWITH, right=other, parents=self.parents
        )

    def endswith(self, other: Any) -> Expression:
        return Expression(
            left=self.field, op=Operators.ENDSWITH, right=other, parents=self.parents
        )

    def contains(self, other: Any) -> Expression:
        return Expression(
            left=self.field, op=Operators.CONTAINS, right=other, parents=self.parents
        )

    def __getattr__(self, item):
        if item.startswith("__"):
            raise AttributeError("cannot invoke __getattr__ with reserved field")
        outer_type = outer_type_or_annotation(self.field)
        if is_supported_container_type(outer_type):
            embedded_cls = get_args(outer_type)
            if not embedded_cls:
                raise QuerySyntaxError(
                    "In order to query on a list field, you must define "
                    "the contents of the list with a type annotation, like: "
                    f"orders: List[Order]. Docs: {ERRORS_URL}#E1"
                )
            embedded_cls = embedded_cls[0]
            attr = getattr(embedded_cls, item)
        else:
            attr = getattr(outer_type, item)
        if isinstance(attr, self.__class__):
            new_parents = self.parents.copy()
            new_parent = (self.field.alias, outer_type)
            if new_parent not in new_parents:
                new_parents.append(new_parent)
            return self.__class__(attr.field, new_parents)
        return attr


class QueryNotSupportedError(Exception):
    """The attempted query is not supported."""


class RediSearchFieldTypes(Enum):
    TEXT = "TEXT"
    TAG = "TAG"
    NUMERIC = "NUMERIC"
    GEO = "GEO"


# Numeric types indexed as NUMERIC RediSearch fields. GEO fields use the
# ``Coordinates`` type and are handled separately in ``FindQuery.resolve_value``
# and ``HashModel.schema_for_type``.
NUMERIC_TYPES = (float, int, decimal.Decimal)
DEFAULT_PAGE_SIZE = 1000


def convert_datetime_to_timestamp(obj):
    """Convert datetime objects to Unix timestamps for storage."""
    if isinstance(obj, dict):
        return {key: convert_datetime_to_timestamp(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_datetime_to_timestamp(item) for item in obj]
    elif isinstance(obj, datetime.datetime):
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=datetime.timezone.utc)
        else:
            obj = obj.astimezone(datetime.timezone.utc)
        return obj.timestamp()
    elif isinstance(obj, datetime.date):
        # Convert date to datetime at midnight and get timestamp
        dt = datetime.datetime.combine(
            obj, datetime.time.min, tzinfo=datetime.timezone.utc
        )
        return dt.timestamp()
    else:
        return obj


def _timestamp_to_datetime(value, target_type):
    """Convert a single numeric/string timestamp to a ``datetime`` or ``date``.

    ``target_type`` must be ``datetime.datetime`` or ``datetime.date``.
    Returns the original value unchanged if it is not a number or if the
    conversion fails (e.g. out-of-range timestamp), so callers can apply this
    opportunistically without losing non-convertible data.
    """
    if not isinstance(value, (int, float, str)):
        return value
    try:
        if isinstance(value, str):
            value = float(value)
        dt = datetime.datetime.fromtimestamp(value, tz=datetime.timezone.utc).replace(
            tzinfo=None
        )
        if target_type is datetime.date:
            return dt.date()
        return dt
    except (ValueError, OSError):
        return value


def convert_timestamp_to_datetime(obj, model_fields):
    """Convert Unix timestamps back to datetime objects based on model field types."""
    if isinstance(obj, dict):
        result = {}
        for key, value in obj.items():
            if key in model_fields:
                field_info = model_fields[key]
                field_type = (
                    field_info.annotation if hasattr(field_info, "annotation") else None
                )

                # Handle Optional types - extract the inner type
                if _is_union_type(field_type):
                    # For Optional[T] (typing.Union[T, None] or PEP 604
                    # T | None), get the non-None type
                    args = get_args(field_type)
                    non_none_types = [
                        arg
                        for arg in args
                        if arg is not type(None)  # noqa: E721
                    ]
                    if len(non_none_types) == 1:
                        field_type = non_none_types[0]

                # Handle direct datetime/date fields
                if field_type in (datetime.datetime, datetime.date):
                    result[key] = _timestamp_to_datetime(value, field_type)
                # Handle nested models - check if it's a RedisModel subclass
                elif isinstance(value, dict):
                    try:
                        # Check if field_type is a class and subclass of RedisModel
                        if isinstance(field_type, type) and has_model_field_mapping(
                            field_type
                        ):
                            result[key] = convert_timestamp_to_datetime(
                                value, get_model_fields(field_type)
                            )
                        else:
                            result[key] = convert_timestamp_to_datetime(value, {})
                    except (TypeError, AttributeError):
                        result[key] = convert_timestamp_to_datetime(value, {})
                # Handle lists that might contain nested models or scalars
                # (e.g. List[SomeModel], List[datetime], List[date])
                elif isinstance(value, list):
                    # Try to extract the inner type from List[...]
                    inner_type = None
                    if (
                        hasattr(field_type, "__origin__")
                        and field_type.__origin__ in (list, List)
                        and hasattr(field_type, "__args__")
                        and field_type.__args__
                    ):
                        inner_type = field_type.__args__[0]

                        # List[datetime] / List[date]: convert each numeric
                        # item back to a datetime/date.
                        if inner_type in (datetime.datetime, datetime.date):
                            result[key] = [
                                _timestamp_to_datetime(item, inner_type)
                                for item in value
                            ]
                        else:
                            # Check if the inner type is a nested model
                            try:
                                if isinstance(
                                    inner_type, type
                                ) and has_model_field_mapping(inner_type):
                                    result[key] = [
                                        convert_timestamp_to_datetime(
                                            item, get_model_fields(inner_type)
                                        )
                                        for item in value
                                    ]
                                else:
                                    result[key] = convert_timestamp_to_datetime(
                                        value, {}
                                    )
                            except (TypeError, AttributeError):
                                result[key] = convert_timestamp_to_datetime(value, {})
                    else:
                        result[key] = convert_timestamp_to_datetime(value, {})
                else:
                    result[key] = convert_timestamp_to_datetime(value, {})
            else:
                # For keys not in model_fields, still recurse but with empty field info
                result[key] = convert_timestamp_to_datetime(value, {})
        return result
    elif isinstance(obj, list):
        return [convert_timestamp_to_datetime(item, model_fields) for item in obj]
    else:
        return obj


def convert_empty_strings_to_none(obj, model_fields):
    """Convert empty strings back to None for Optional fields in HashModel.

    HashModel stores None as empty string "" because Redis HSET requires non-null
    values. This function converts empty strings back to None for fields that are
    Optional (Union[T, None]) so Pydantic validation succeeds. (Fixes #254)
    """
    if not isinstance(obj, dict):
        return obj

    result = {}
    for key, value in obj.items():
        if key in model_fields and value == "":
            field_info = model_fields[key]
            field_type = (
                field_info.annotation if hasattr(field_info, "annotation") else None
            )
            # Check if the field is Optional (typing.Union[T, None] or PEP 604 T | None)
            is_optional = False
            if _is_union_type(field_type):
                args = get_args(field_type)
                if type(None) in args:
                    is_optional = True

            if is_optional:
                result[key] = None
            else:
                result[key] = value
        else:
            result[key] = value
    return result


def convert_bytes_to_base64(obj):
    """Convert bytes objects to base64-encoded strings for storage.

    This is necessary because Redis JSON and the jsonable_encoder cannot
    handle arbitrary binary data. Base64 encoding ensures all byte values
    (0-255) can be safely stored and retrieved.
    """
    import base64

    if isinstance(obj, dict):
        return {key: convert_bytes_to_base64(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_bytes_to_base64(item) for item in obj]
    elif isinstance(obj, bytes):
        return base64.b64encode(obj).decode("ascii")
    else:
        return obj


def convert_dataclasses_to_dicts(obj):
    """Recursively convert non-JSON-serializable types to JSON-safe values.

    Handles:
    - ``Coordinates`` → ``"lon,lat"`` string (required by RediSearch GEO)
    - Other dataclasses → plain dicts via ``dataclasses.asdict()``
    - ``set`` / ``frozenset`` → ``list``
    - ``uuid.UUID`` → string
    - ``Enum`` → its ``.value``
    - ``decimal.Decimal`` → ``float``

    Pydantic v1's ``.dict()`` does not automatically serialise these types,
    so this helper must run before ``json().set()``.
    """
    import uuid

    if isinstance(obj, Coordinates):
        return str(obj)  # "lon,lat" string for RediSearch GEO
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    if isinstance(obj, dict):
        return {key: convert_dataclasses_to_dicts(value) for key, value in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [convert_dataclasses_to_dicts(item) for item in obj]
    if isinstance(obj, (set, frozenset)):
        return [convert_dataclasses_to_dicts(item) for item in obj]
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    return obj


def convert_base64_to_bytes(obj, model_fields):
    """Convert base64-encoded strings back to bytes based on model field types."""
    import base64

    if not isinstance(obj, dict):
        return obj

    result = {}
    for key, value in obj.items():
        if key in model_fields:
            field_info = model_fields[key]
            field_type = (
                field_info.annotation if hasattr(field_info, "annotation") else None
            )

            # Handle Optional types - extract the inner type
            if _is_union_type(field_type):
                args = get_args(field_type)
                non_none_types = [
                    arg
                    for arg in args
                    if arg is not type(None)  # noqa: E721
                ]
                if len(non_none_types) == 1:
                    field_type = non_none_types[0]

            if field_type is bytes and isinstance(value, str):
                try:
                    result[key] = base64.b64decode(value)
                except (ValueError, TypeError):
                    result[key] = value
            elif isinstance(value, dict):
                # Handle nested models with bytes fields
                try:
                    if isinstance(field_type, type) and has_model_field_mapping(
                        field_type
                    ):
                        result[key] = convert_base64_to_bytes(
                            value, get_model_fields(field_type)
                        )
                    else:
                        result[key] = value
                except (TypeError, AttributeError):
                    result[key] = value
            elif isinstance(value, list):
                # Handle lists that might contain nested models with bytes
                inner_type = None
                if (
                    hasattr(field_type, "__origin__")
                    and field_type.__origin__ in (list, List)
                    and hasattr(field_type, "__args__")
                    and field_type.__args__
                ):
                    inner_type = field_type.__args__[0]
                    try:
                        if isinstance(inner_type, type) and has_model_field_mapping(
                            inner_type
                        ):
                            result[key] = [
                                convert_base64_to_bytes(
                                    item, get_model_fields(inner_type)
                                )
                                for item in value
                            ]
                        else:
                            result[key] = value
                    except (TypeError, AttributeError):
                        result[key] = value
                else:
                    result[key] = value
            else:
                result[key] = value
        else:
            result[key] = value
    return result


class FindQueryCursor:
    def __init__(
        self,
        model: Type["RedisModel"],
        index_name: str,
        cursor_id: int,
        count: int,
        results: Optional[Sequence["RedisModel"]] = None,
        total: Optional[int] = None,
    ):
        from collections import deque

        self.model = model
        self.index_name = index_name
        self.cursor_id = cursor_id
        self.count = count
        self.total = total
        self._buffer: deque = deque(results or [])

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._buffer:
            self._buffer.extend(await self.read())
        if not self._buffer:
            raise StopAsyncIteration
        return self._buffer.popleft()

    @property
    def exhausted(self) -> bool:
        return self.cursor_id == 0 and not self._buffer

    def token(self, secret: Optional[Union[str, bytes]] = None) -> str:
        """Serialize cursor state into a URL-safe token.

        Pass a secret in web applications so the token is signed and cannot be
        tampered with by clients. Unsigned tokens are intended for trusted
        server-side handoff only because the cursor id is a Redis server-side
        resource identifier.
        """
        payload = {
            "index_name": self.index_name,
            "cursor_id": self.cursor_id,
            "count": self.count,
        }
        body = _urlsafe_b64encode(
            json.dumps(payload, separators=(",", ":")).encode("utf-8")
        )
        if secret is None:
            return body
        secret_bytes = secret.encode("utf-8") if isinstance(secret, str) else secret
        signature = hmac.new(secret_bytes, body.encode("ascii"), hashlib.sha256)
        return f"{body}.{_urlsafe_b64encode(signature.digest())}"

    @classmethod
    def from_token(
        cls,
        model: Type["RedisModel"],
        token: str,
        secret: Optional[Union[str, bytes]] = None,
    ) -> "FindQueryCursor":
        parts = token.split(".")
        if secret is None:
            if len(parts) != 1:
                raise ValueError(
                    "Token appears to be signed but no secret was provided."
                )
            body = parts[0]
        else:
            if len(parts) != 2:
                raise ValueError(
                    "Invalid cursor token format: expected unsigned or signed token."
                )
            body, signature = parts
            secret_bytes = secret.encode("utf-8") if isinstance(secret, str) else secret
            expected = hmac.new(
                secret_bytes, body.encode("ascii"), hashlib.sha256
            ).digest()
            actual = _urlsafe_b64decode(signature)
            if not hmac.compare_digest(actual, expected):
                raise ValueError("Cursor token signature is invalid.")

        payload = json.loads(_urlsafe_b64decode(body).decode("utf-8"))
        if payload["index_name"] != model.Meta.index_name:
            raise ValueError(
                "Cursor token index mismatch: expected "
                f"{model.Meta.index_name}, got {payload['index_name']}."
            )
        return cls(
            model=model,
            index_name=payload["index_name"],
            cursor_id=int(payload["cursor_id"]),
            count=int(payload["count"]),
        )

    async def read(self) -> Sequence["RedisModel"]:
        if self._buffer:
            results = list(self._buffer)
            self._buffer.clear()
            return results
        if self.cursor_id == 0:
            return []
        raw_result = await self.model.db().execute_command(
            "FT.CURSOR",
            "READ",
            self.index_name,
            self.cursor_id,
            "COUNT",
            self.count,
        )
        protocol = protocol_version(self.model.db())
        aggregate_result, self.cursor_id = self._split_cursor_result(
            raw_result, protocol=protocol
        )
        return await self._models_from_aggregate_result(self.model, aggregate_result)

    async def all(self) -> Sequence["RedisModel"]:
        """Read every remaining cursor page into memory.

        Prefer page-by-page ``read()`` or async iteration for unbounded result
        sets.
        """
        results: List["RedisModel"] = []
        while True:
            page = await self.read()
            if not page:
                break
            results.extend(page)
        return results

    async def close(self) -> None:
        if self.cursor_id == 0:
            return
        try:
            await self.model.db().execute_command(
                "FT.CURSOR", "DEL", self.index_name, self.cursor_id
            )
        finally:
            self.cursor_id = 0

    @staticmethod
    def _split_cursor_result(
        raw_result: Any, protocol: Optional[int] = None
    ) -> Tuple[Any, int]:
        rows, cursor_id = split_cursor_response(raw_result, protocol=protocol)
        return rows, cursor_id

    @staticmethod
    def _aggregate_total(aggregate_result: Any, protocol: Optional[int] = None) -> int:
        # RESP3 produces a dict; RESP2 produces a flat list starting with the
        # group/document count.  split_search_response normalises both;
        # pass command="aggregate" so the RESP2 parser uses the
        # FT.AGGREGATE layout ([count, row1, row2, ...]) instead of the
        # FT.SEARCH layout.
        try:
            total, _ = split_search_response(
                aggregate_result, protocol=protocol, command="aggregate"
            )
            return total
        except (IndexError, TypeError, ValueError):
            return 0

    @staticmethod
    def _extract_key(row: Any) -> Optional[str]:
        if isinstance(row, dict):
            extra = row.get("extra_attributes") or {}
            if isinstance(extra, dict):
                value = extra.get("__key")
                if value is not None:
                    return _decode_token_value(value)
            # RESP3 ``values`` is ``[name, value, name, value, ...]``.
            values = row.get("values") or []
            if isinstance(values, list):
                for i in range(0, len(values), 2):
                    if i + 1 >= len(values):
                        break
                    name = _decode_token_value(values[i])
                    if name == "__key":
                        return _decode_token_value(values[i + 1])
            return None
        return extract_key_from_row(row)

    @classmethod
    def _pk_from_redis_key(cls, model: Type["RedisModel"], key: str) -> str:
        key_prefix = model.make_key(model._meta.primary_key_pattern.format(pk=""))
        return remove_prefix(key, key_prefix)

    @classmethod
    async def _models_from_aggregate_result(
        cls, model: Type["RedisModel"], aggregate_result: Any
    ) -> Sequence["RedisModel"]:
        # ``aggregate_result`` is already normalised by ``_split_cursor_result``
        # into a list of ``[key, fields_list]`` rows; RESP3 dicts are converted
        # to the same shape by ``split_cursor_response``.  RESP2 callers may
        # still pass the raw ``[count, key, fields, ...]`` shape, in which case
        # we slice off the leading count.
        if (
            aggregate_result
            and isinstance(aggregate_result, list)
            and not (
                len(aggregate_result) > 0
                and isinstance(aggregate_result[0], (list, dict))
            )
        ):
            rows = aggregate_result[1:]
        else:
            rows = aggregate_result or []
        pks = []
        for row in rows:
            key = cls._extract_key(row)
            if key is not None:
                pks.append(cls._pk_from_redis_key(model, key))
        return await model.get_many(pks)


class FindQuery:
    def __init__(
        self,
        expressions: Sequence[ExpressionOrNegated],
        model: Type["RedisModel"],
        knn: Optional[KNNExpression] = None,
        offset: int = 0,
        limit: Optional[int] = None,
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_fields: Optional[List[str]] = None,
        nocontent: bool = False,
    ):
        self.expressions = expressions
        self.model = model
        self.knn = knn
        self.offset = offset
        self.limit = limit or (self.knn.k if self.knn else DEFAULT_PAGE_SIZE)
        self.page_size = page_size
        self.nocontent = nocontent

        if sort_fields:
            self.sort_fields = self.validate_sort_fields(sort_fields)
        elif self.knn:
            self.sort_fields = [self.knn.score_field]
        else:
            self.sort_fields = []

        self._expression = None
        self._query: Optional[str] = None
        self._pagination: List[str] = []
        self._model_cache: List[RedisModel] = []

    def dict(self) -> Dict[str, Any]:
        return dict(
            model=self.model,
            offset=self.offset,
            page_size=self.page_size,
            limit=self.limit,
            expressions=copy(self.expressions),
            sort_fields=copy(self.sort_fields),
            nocontent=self.nocontent,
        )

    def copy(self, **kwargs):
        original = self.dict()
        original.update(**kwargs)
        # When sort_fields is not explicitly overridden by the caller (e.g. the
        # transparent pagination loop in execute() or sort_by chaining without
        # changing fields), the existing sort_fields are already resolved
        # (e.g. embedded paths like "metrics.score" have been flattened to
        # "metrics_score"). Re-running validate_sort_fields() on the resolved
        # form would fail for embedded paths because the flattened name does
        # not exist in model_fields. Bypass __init__ validation in that case
        # by removing sort_fields from the kwargs and reattaching the already-
        # validated list on the new instance.
        sort_fields_overridden = "sort_fields" in kwargs
        preserved_sort_fields = (
            None if sort_fields_overridden else original.pop("sort_fields", None)
        )
        new_query = FindQuery(**original)
        if preserved_sort_fields is not None:
            new_query.sort_fields = list(preserved_sort_fields)
        return new_query

    @property
    def pagination(self):
        if self._pagination:
            return self._pagination
        self._pagination = self.resolve_redisearch_pagination()
        return self._pagination

    @property
    def expression(self):
        if self._expression:
            return self._expression
        if self.expressions:
            self._expression = reduce(operator.and_, self.expressions)
        else:
            self._expression = Expression(
                left=None, right=None, op=Operators.ALL, parents=[]
            )
        return self._expression

    @property
    def query(self):
        """
        Resolve and return the RediSearch query for this FindQuery.

        NOTE: We cache the resolved query string after generating it. This should be OK
        because all mutations of FindQuery through public APIs return a new FindQuery instance.
        """
        if self._query:
            return self._query
        self._query = self.resolve_redisearch_query(self.expression)
        if self.knn:
            # Always wrap the filter expression in parentheses when combining
            # with KNN, unless it's the wildcard "*". This ensures OR expressions
            # like "(A)| (B)" become "((A)| (B))=>[KNN ...]" instead of the
            # invalid "(A)| (B)=>[KNN ...]" where KNN only applies to the
            # second term.
            if self._query != "*":
                self._query = f"({self._query})"
            self._query += f"=>[{self.knn}]"
        return self._query

    @property
    def query_params(self):
        params: List[Union[str, bytes]] = []
        if self.knn:
            params += [attr for kv in self.knn.query_params.items() for attr in kv]
        return params

    def validate_sort_fields(self, sort_fields: List[str]):
        resolved_sort_fields = []
        for sort_field in sort_fields:
            field_name = sort_field.lstrip("-")
            if self.knn and field_name == self.knn.score_field:
                resolved_sort_fields.append(sort_field)
                continue
            resolved_field_name, field_info = self.resolve_sort_field(field_name)
            if not getattr(field_info, "sortable", False):
                raise QueryNotSupportedError(
                    f"You tried sort by {field_name}, but {self.model} does "
                    f"not define that field as sortable. Docs: {ERRORS_URL}#E2"
                )
            resolved_sort_fields.append(
                f"-{resolved_field_name}"
                if sort_field.startswith("-")
                else resolved_field_name
            )
        return resolved_sort_fields

    def resolve_sort_field(self, field_name: str) -> Tuple[str, PydanticFieldInfo]:
        # Queries use `.` or `__` for embedded paths, but RediSearch SORTBY uses
        # the flattened schema alias with underscores.
        normalized_field_name = field_name.replace(".", "__")
        parts = normalized_field_name.split("__")
        current_model = self.model
        resolved_parts = []
        field_info = None

        for index, part in enumerate(parts):
            if part not in current_model.model_fields:
                raise QueryNotSupportedError(
                    f"You tried sort by {field_name}, but that field "
                    f"does not exist on the model {self.model}"
                )
            field = current_model.model_fields[part]
            resolved_parts.append(part)
            field_info = field

            if index == len(parts) - 1:
                break

            field_type = outer_type_or_annotation(field)
            if is_supported_container_type(field_type):
                type_args = get_args(field_type)
                field_type = type_args[0] if type_args else field_type
            if not isinstance(field_type, type) or not issubclass(
                field_type, RedisModel
            ):
                raise QueryNotSupportedError(
                    f"You tried sort by {field_name}, but that field "
                    f"does not exist on the model {self.model}"
                )
            current_model = field_type

        if field_info is None:
            raise QueryNotSupportedError(
                f"You tried sort by {field_name}, but that field "
                f"does not exist on the model {self.model}"
            )
        return "_".join(resolved_parts), field_info

    @staticmethod
    def resolve_field_type(
        field: Union[ModelField, PydanticFieldInfo], op: Operators
    ) -> RediSearchFieldTypes:
        field_info: Union[FieldInfo, ModelField, PydanticFieldInfo]

        if not hasattr(field, "field_info"):
            field_info = field
        else:
            field_info = field.field_info
        if getattr(field_info, "primary_key", None) is True:
            return RediSearchFieldTypes.TAG
        elif op is Operators.LIKE:
            fts = getattr(field_info, "full_text_search", None)
            if fts is not True:  # Could be PydanticUndefined
                raise QuerySyntaxError(
                    f"You tried to do a full-text search on the field '{field.alias}', "
                    f"but the field is not indexed for full-text search. Use the "
                    f"full_text_search=True option. Docs: {ERRORS_URL}#E3"
                )
            return RediSearchFieldTypes.TEXT

        field_type = outer_type_or_annotation(field)

        if field_type is Coordinates:
            return RediSearchFieldTypes.GEO
        container_type = get_origin(field_type)
        # Literal annotations have an origin but are indexed as scalar TAG fields.
        if container_type is Literal:
            return RediSearchFieldTypes.TAG

        if is_supported_container_type(field_type):
            # NOTE: A list of strings, like:
            #
            #     tarot_cards: List[str] = field(index=True)
            #
            # becomes a TAG field, which means that users can run equality and
            # membership queries on values.
            #
            # Meanwhile, a list of RedisModels, like:
            #
            #     friends: List[Friend] = field(index=True)
            #
            # is not itself directly indexed, but instead, we index any fields
            # within the model inside the list marked as `index=True`.
            return RediSearchFieldTypes.TAG
        elif container_type is not None:
            raise QuerySyntaxError(
                "Only lists and tuples are supported for multi-value fields. "
                f"Docs: {ERRORS_URL}#E4"
            )
        elif field_type is bool:
            return RediSearchFieldTypes.TAG
        elif is_numeric_type(field_type):
            # Index numeric Python types as NUMERIC fields, so we can support
            # range queries.
            return RediSearchFieldTypes.NUMERIC
        else:
            # TAG fields are the default field type and support equality and
            # membership queries, though membership (and the multi-value nature
            # of the field) are hidden from users unless they explicitly index
            # multiple values, with either a list or tuple,
            # e.g.,
            #    favorite_foods: List[str] = field(index=True)
            return RediSearchFieldTypes.TAG

    @staticmethod
    def expand_tag_value(value):
        if isinstance(value, str):
            return escaper.escape(value)
        if isinstance(value, bytes):
            # Bytes values are passed through unchanged. Decoding to ``str``
            # would be lossy without knowing the original encoding, and TAG
            # values are byte-comparable on the Redis side. Note that RediSearch
            # TAG fields only accept strings, so a list of bytes saved into a
            # TAG-indexed array will fail at index time; callers should convert
            # such values to strings before saving.
            return value
        try:
            return "|".join([escaper.escape(str(v)) for v in value])
        except TypeError:
            log.debug(
                "Escaping single non-iterable value used for an IN or NOT_IN query: %s",
                value,
            )
        return escaper.escape(str(value))

    @staticmethod
    def _get_embedded_model_class(
        field: ModelField,
    ) -> Optional[Type["RedisModel"]]:
        field_type = outer_type_or_annotation(field)
        if not is_supported_container_type(field_type):
            return None

        args = get_args(field_type)
        if not args:
            return None

        embedded_cls = _unwrap_type_annotation(args[0])
        try:
            if issubclass(embedded_cls, RedisModel):
                return embedded_cls
        except TypeError:
            return None
        return None

    @staticmethod
    def _normalize_embedded_query_values(value: Any) -> Optional[List[Any]]:
        if isinstance(value, (dict, RedisModel)):
            return [value]
        if (
            isinstance(value, (list, tuple))
            and value
            and all(isinstance(item, (dict, RedisModel)) for item in value)
        ):
            return list(value)
        return None

    @staticmethod
    def _get_non_none_query_fields(value: Any) -> Dict[str, Any]:
        # RediSearch does not index JSON null values, so None-valued criteria
        # cannot produce a matching field query.
        if isinstance(value, RedisModel):
            return value.model_dump(exclude_unset=True, exclude_none=True)
        return {key: val for key, val in value.items() if val is not None}

    @classmethod
    def resolve_embedded_model_container_query(
        cls,
        field: ModelField,
        value: Any,
        parents: List[Tuple[str, "RedisModel"]],
    ) -> Optional[str]:
        embedded_cls = cls._get_embedded_model_class(field)
        if embedded_cls is None:
            return None
        if isinstance(value, (list, tuple)) and not value:
            raise QuerySyntaxError(
                f"Cannot query embedded model list field {field.alias!r} with an "
                "empty list. Provide at least one query criterion."
            )
        values = cls._normalize_embedded_query_values(value)
        if values is None:
            return None

        parent_type = outer_type_or_annotation(field)
        new_parents = parents.copy()
        new_parent = (field.alias, parent_type)
        if new_parent not in new_parents:
            new_parents.append(new_parent)

        queries = []
        is_embedded = getattr(getattr(embedded_cls, "_meta", None), "embedded", False)
        for query_value in values:
            field_values = cls._get_non_none_query_fields(query_value)
            parts = []
            for name, field_value in field_values.items():
                field_info = embedded_cls.model_fields.get(name)
                if field_info is None:
                    aliased_field = next(
                        (
                            (field_name, field)
                            for field_name, field in embedded_cls.model_fields.items()
                            if field.alias == name
                        ),
                        None,
                    )
                    if aliased_field is None:
                        raise QuerySyntaxError(
                            f"Field {name!r} is not defined on embedded model "
                            f"{embedded_cls.__name__}. Available fields: "
                            f"{list(embedded_cls.model_fields.keys())}."
                        )
                    name, field_info = aliased_field
                if name == "pk" and is_embedded:
                    continue
                if not getattr(field_info, "index", False):
                    raise QueryNotSupportedError(
                        f"You tried to query by a field ({field.alias}_{name}) "
                        f"that isn't indexed. Docs: {ERRORS_URL}#E6"
                    )
                op = (
                    Operators.IN
                    if isinstance(field_value, (list, tuple, set))
                    else Operators.EQ
                )
                field_type = cls.resolve_field_type(field_info, op)
                parts.append(
                    cls.resolve_value(
                        name,
                        field_type,
                        field_info,
                        op,
                        field_value,
                        new_parents,
                    )
                )
            if not parts:
                indexed_fields = [
                    name
                    for name, field in embedded_cls.model_fields.items()
                    if getattr(field, "index", False)
                ]
                raise QuerySyntaxError(
                    f"No indexed fields were provided for embedded model "
                    f"{embedded_cls.__name__}. Available indexed fields: "
                    f"{indexed_fields}."
                )
            queries.append(" ".join(parts))

        if len(queries) == 1:
            return queries[0]
        return "| ".join(f"({query})" for query in queries)

    @classmethod
    def resolve_value(
        cls,
        field_name: str,
        field_type: RediSearchFieldTypes,
        field_info: PydanticFieldInfo,
        op: Operators,
        value: Any,
        parents: List[Tuple[str, "RedisModel"]],
    ) -> str:
        result = ""
        if parents:
            prefix = "_".join([p[0] for p in parents])
            field_name = f"{prefix}_{field_name}"
        if field_type is RediSearchFieldTypes.TEXT:
            result = f"@{field_name}_fts:"
            if op is Operators.EQ:
                result += f'"{escaper.escape(value)}"'
            elif op is Operators.NE:
                result = f'-({result}"{escaper.escape(value)}")'
            elif op is Operators.LIKE:
                result += value
            else:
                raise QueryNotSupportedError(
                    "Only equals (=), not-equals (!=), and like() "
                    "comparisons are supported for TEXT fields. "
                    f"Docs: {ERRORS_URL}#E5"
                )
        elif field_type is RediSearchFieldTypes.NUMERIC:

            def convert_numeric_value(v):
                """Convert Enum and datetime values for NUMERIC queries."""
                # Convert Enum to its value (fixes #108)
                if isinstance(v, Enum):
                    v = v.value
                # Convert datetime objects to timestamps
                if isinstance(v, (datetime.datetime, datetime.date)):
                    if isinstance(v, datetime.date) and not isinstance(
                        v, datetime.datetime
                    ):
                        v = datetime.datetime.combine(v, datetime.time.min)
                    v = v.timestamp()
                return v

            if op is Operators.IN:
                # Handle IN operator for NUMERIC fields (fixes #499)
                converted_values = [convert_numeric_value(v) for v in value]
                parts = [f"(@{field_name}:[{v} {v}])" for v in converted_values]
                result += "|".join(parts)
            elif op is Operators.NOT_IN:
                # Handle NOT_IN operator for NUMERIC fields
                converted_values = [convert_numeric_value(v) for v in value]
                parts = [f"(@{field_name}:[{v} {v}])" for v in converted_values]
                result += f"-({' | '.join(parts)})"
            else:
                value = convert_numeric_value(value)
                if op is Operators.EQ:
                    result += f"@{field_name}:[{value} {value}]"
                elif op is Operators.NE:
                    result += f"-(@{field_name}:[{value} {value}])"
                elif op is Operators.GT:
                    result += f"@{field_name}:[({value} +inf]"
                elif op is Operators.LT:
                    result += f"@{field_name}:[-inf ({value}]"
                elif op is Operators.GE:
                    result += f"@{field_name}:[{value} +inf]"
                elif op is Operators.LE:
                    result += f"@{field_name}:[-inf {value}]"
        # NOTE: Both "multi-value" TAG indexes (user-facing lists indexed as
        # a single TAG field) and single-value exact-match fields (a regular
        # ``str = Field(index=True)``) are stored as TAG internally. Both render
        # the same query syntax here. There is no reliable way to distinguish
        # them at this layer without inspecting the model schema, which is why
        # users sometimes see surprising results when querying a single-value
        # field with multiple values via ``IN``.
        elif field_type is RediSearchFieldTypes.GEO:
            if not isinstance(value, GeoFilter):
                raise QuerySyntaxError(
                    "You can only use a GeoFilter object with a GEO field."
                )
            if op is Operators.EQ:
                result += f"@{field_name}:[{value}]"
        elif field_type is RediSearchFieldTypes.TAG:
            if op is Operators.EQ:
                separator_char = getattr(
                    field_info, "separator", SINGLE_VALUE_TAG_FIELD_SEPARATOR
                )
                if value == separator_char:
                    # The value is ONLY the TAG field separator character --
                    # this is not going to work.
                    log.warning(
                        "Your query against the field %s is for a single character, %s, "
                        "that is used internally by redis-om-python. We must ignore "
                        "this portion of the query. Please review your query to find "
                        "an alternative query that uses a string containing more than "
                        "just the character %s.",
                        field_name,
                        separator_char,
                        separator_char,
                    )
                    return ""
                if isinstance(value, bool):
                    result = "@{field_name}:{{{value}}}".format(
                        field_name=field_name, value=value
                    )
                elif isinstance(value, int):
                    # Integer primary-key queries use NUMERIC range syntax (intentionally).
                    result = f"@{field_name}:[{value} {value}]"
                elif separator_char in value:
                    # The value contains the TAG field separator. We can work
                    # around this by breaking apart the values and unioning them
                    # with multiple field:{} queries.
                    sub_values: list = [v for v in value.split(separator_char) if v]
                    parts = [
                        "@{field_name}:{{{v}}}".format(
                            field_name=field_name, v=escaper.escape(v)
                        )
                        for v in sub_values
                    ]
                    result += " ".join(parts)
                else:
                    value = escaper.escape(value)
                    result += "@{field_name}:{{{value}}}".format(
                        field_name=field_name, value=value
                    )
            elif op is Operators.NE:
                value = escaper.escape(value)
                result += "-(@{field_name}:{{{value}}})".format(
                    field_name=field_name, value=value
                )
            elif op is Operators.IN:
                expanded_value = cls.expand_tag_value(value)
                result += "(@{field_name}:{{{expanded_value}}})".format(
                    field_name=field_name, expanded_value=expanded_value
                )
            elif op is Operators.NOT_IN:
                expanded_value = cls.expand_tag_value(value)
                result += "-(@{field_name}:{{{expanded_value}}})".format(
                    field_name=field_name, expanded_value=expanded_value
                )
            elif op is Operators.STARTSWITH:
                expanded_value = cls.expand_tag_value(value)
                result += "(@{field_name}:{{{expanded_value}*}})".format(
                    field_name=field_name, expanded_value=expanded_value
                )
            elif op is Operators.ENDSWITH:
                expanded_value = cls.expand_tag_value(value)
                result += "(@{field_name}:{{*{expanded_value}}})".format(
                    field_name=field_name, expanded_value=expanded_value
                )
            elif op is Operators.CONTAINS:
                expanded_value = cls.expand_tag_value(value)
                result += "(@{field_name}:{{*{expanded_value}*}})".format(
                    field_name=field_name, expanded_value=expanded_value
                )

        return result

    def resolve_redisearch_pagination(self):
        """Resolve pagination options for a query."""
        return ["LIMIT", self.offset, self.limit]

    def resolve_redisearch_sort_fields(self):
        """Resolve sort options for a query."""
        if not self.sort_fields:
            return
        fields = []
        for f in self.sort_fields:
            direction = "desc" if f.startswith("-") else "asc"
            fields.extend([f.lstrip("-"), direction])
        if self.sort_fields:
            return ["SORTBY", *fields]

    def resolve_redisearch_aggregate_sort_fields(self):
        """Resolve FT.AGGREGATE sort options for a query."""
        if not self.sort_fields:
            return
        fields = []
        for f in self.sort_fields:
            direction = "DESC" if f.startswith("-") else "ASC"
            field_name = f.lstrip("-")
            fields.extend([f"@{field_name}", direction])
        return ["SORTBY", str(len(fields)), *fields]

    @classmethod
    def resolve_redisearch_query(cls, expression: ExpressionOrNegated) -> str:
        """
        Resolve an arbitrarily deep expression into a single RediSearch query string.

        This method is complex. Note the following:

        1. This method makes a recursive call to itself when it finds that
           either the left or right operand contains another expression.

        2. An expression might be in a "negated" form, which means that the user
           gave us an expression like ~(Member.age == 30), or in other words,
           "Members whose age is NOT 30." Thus, a negated expression is one in
           which the meaning of an expression is inverted. If we find a negated
           expression, we need to add the appropriate "NOT" syntax but can
           otherwise use the resolved RediSearch query for the expression as-is.

        3. The final resolution of an expression should be a left operand that's
           a ModelField, an operator, and a right operand that's NOT a ModelField.
           With an IN or NOT_IN operator, the right operand can be a sequence
           type, but otherwise, sequence types are rejected to surface likely
           user errors (e.g. accidentally passing a list to an equality check).
        """
        field_type = None
        field_name = None
        field_info = None
        encompassing_expression_is_negated = False
        result = ""

        if isinstance(expression, NegatedExpression):
            encompassing_expression_is_negated = True
            expression = expression.expression

        # Logical operators (Or / And / Not) build a RediSearch query string
        # from a list of sub-expressions. Delegate to their own ``query``
        # property, which is the single source of truth for their rendering.
        # Deferred import avoids a circular dependency (query_resolver imports
        # from this module).
        from aredis_om.model.query_resolver import (
            LogicalOperatorForListOfExpressions,
        )

        if isinstance(expression, LogicalOperatorForListOfExpressions):
            return expression.query

        if expression.op is Operators.ALL:
            if encompassing_expression_is_negated:
                # Negating a query-for-all-results ("*") would mean "return all
                # documents that don't match every document," which is logically
                # contradictory. Users who actually want a filter-then-negate
                # workflow should construct a positive query expression and
                # negate that. A full-text-search scoring use case is unlikely
                # to be useful here, so we explicitly raise.
                raise QueryNotSupportedError(
                    "You cannot negate a query for all results."
                )
            return "*"

        if isinstance(expression.left, Expression) or isinstance(
            expression.left, NegatedExpression
        ):
            result += f"({cls.resolve_redisearch_query(expression.left)})"
        elif is_model_field_instance(expression.left):
            field_type = cls.resolve_field_type(expression.left, expression.op)
            field_name = expression.left.name
            field_info = expression.left.field_info
            resolved_field_name = field_name
            if expression.parents:
                prefix = "_".join([p[0] for p in expression.parents])
                resolved_field_name = f"{prefix}_{field_name}"
            if not field_info or not getattr(field_info, "index", None):
                raise QueryNotSupportedError(
                    f"You tried to query by a field ({resolved_field_name}) "
                    f"that isn't indexed. Docs: {ERRORS_URL}#E6"
                )
        elif isinstance(expression.left, FieldInfo):
            field_type = cls.resolve_field_type(expression.left, expression.op)
            field_name = expression.left.alias
            field_info = expression.left
            if not field_info or not getattr(field_info, "index", None):
                raise QueryNotSupportedError(
                    f"You tried to query by a field ({field_name}) "
                    f"that isn't indexed. Docs: {ERRORS_URL}#E6"
                )
        else:
            raise QueryNotSupportedError(
                "A query expression should start with either a field "
                f"or an expression enclosed in parentheses. Docs: {ERRORS_URL}#E7"
            )

        right = expression.right

        if isinstance(right, Expression) or isinstance(right, NegatedExpression):
            if expression.op == Operators.AND:
                result += " "
            elif expression.op == Operators.OR:
                result += "| "
            else:
                raise QueryNotSupportedError(
                    "You can only combine two query expressions with"
                    f"AND (&) or OR (|). Docs: {ERRORS_URL}#E8"
                )

            if isinstance(right, NegatedExpression):
                result += "-"
                # We're handling the RediSearch operator in this call ("-"), so resolve the
                # inner expression instead of the NegatedExpression.
                right = right.expression

            result += f"({cls.resolve_redisearch_query(right)})"
        else:
            if not field_name:
                raise QuerySyntaxError("Could not resolve field name.")
            elif not field_type:
                raise QuerySyntaxError("Could not resolve field type.")
            elif not field_info:
                raise QuerySyntaxError("Could not resolve field info.")
            elif is_model_field_instance(right):
                raise QueryNotSupportedError("Comparing fields is not supported.")
            elif (
                expression.op not in (Operators.IN, Operators.NOT_IN)
                and not isinstance(right, (str, bytes))
                and isinstance(right, collections.abc.Sequence)
            ):
                raise QueryNotSupportedError(
                    f"You passed a sequence value ({right!r}) to operator "
                    f"{expression.op.name}. Only IN (<<) and NOT_IN (>>) support "
                    f"sequence values. Use those operators, or pass a single "
                    f"value. Docs: {ERRORS_URL}#E9"
                )
            else:
                embedded_query = None
                if is_model_field_instance(expression.left) and expression.op in (
                    Operators.EQ,
                    Operators.IN,
                ):
                    embedded_query = cls.resolve_embedded_model_container_query(
                        expression.left,
                        right,
                        expression.parents,
                    )
                result += embedded_query or cls.resolve_value(
                    field_name,
                    field_type,
                    field_info,
                    expression.op,
                    right,
                    expression.parents,
                )

        if encompassing_expression_is_negated:
            result = f"-({result})"

        return result

    async def execute(
        self, exhaust_results=True, return_raw_result=False, return_query_args=False
    ):
        args: List[Union[str, bytes]] = [  # type: ignore
            "FT.SEARCH",
            self.model.Meta.index_name,
            self.query,
            *self.pagination,
        ]
        if self.sort_fields:
            args += self.resolve_redisearch_sort_fields()

        if self.query_params:
            args += ["PARAMS", str(len(self.query_params))] + self.query_params

        if self.knn:
            # Ensure DIALECT is at least 2
            if "DIALECT" not in args:
                args += ["DIALECT", "2"]
            else:
                i_dialect = args.index("DIALECT") + 1
                if int(args[i_dialect]) < 2:
                    args[i_dialect] = "2"
            args += ["RETURN", "2", "$", self.knn.score_field]

        if self.nocontent:
            args.append("NOCONTENT")

        if return_query_args:
            return self.model.Meta.index_name, args

        if not await has_redisearch(self.model.db()):
            raise RedisModelError(
                "Your Redis instance does not have either the RediSearch module "
                "or RedisJSON module installed. Querying requires that your Redis "
                "instance has one of these modules installed."
            )
        if not getattr(self.model._meta, "index_health_checked", False):
            await self.model.check_index_health()
            self.model._meta.index_health_checked = True

        # Reset the cache if we're executing from offset 0.
        if self.offset == 0:
            self._model_cache.clear()

        # If the offset is greater than 0, we're paginating through a result set,
        # so append the new results to results already in the cache.
        raw_result = await self.model.db().execute_command(*args)
        if return_raw_result:
            return raw_result
        protocol = protocol_version(self.model.db())
        count, _ = split_search_response(raw_result, protocol=protocol)
        results = self.model.from_redis(raw_result, protocol=protocol)
        self._model_cache += results

        if not exhaust_results:
            return self._model_cache

        # The query returned all results, so we have no more work to do.
        if count <= len(results):
            return self._model_cache

        # Transparently (to the user) make subsequent requests to paginate
        # through the results and finally return them all.
        query = self
        while True:
            # Make a query for each pass of the loop, with a new offset equal to the
            # current offset plus `limit`, until we stop getting results back.
            query = query.copy(offset=query.offset + query.limit)
            _results = await query.execute(exhaust_results=False)
            if not _results:
                break
            self._model_cache += _results
        return self._model_cache

    async def get_query(self):
        query = self.copy()
        return await query.execute(return_query_args=True)

    async def first(self):
        query = self.copy(offset=0, limit=1, sort_fields=self.sort_fields)
        results = await query.execute(exhaust_results=False)
        if not results:
            raise NotFoundError()
        return results[0]

    async def count(self):
        query = self.copy(offset=0, limit=0, nocontent=True)
        result = await query.execute(exhaust_results=True, return_raw_result=True)
        # ``result`` is the raw FT.SEARCH payload, whose total is at index 0
        # for RESP2 and inside ``total_results`` for RESP3.
        protocol = protocol_version(self.model.db())
        total, _ = split_search_response(result, protocol=protocol)
        return total

    async def aggregate_ct(self) -> int:
        # WARN: Has issues when multiple 'find' parameters match the same record
        #       It will count them more than once.
        args = [
            "FT.AGGREGATE",
            self.model.Meta.index_name,
            self.query,
            "APPLY",
            "matched_terms()",
            "AS",
            "countable",
            "GROUPBY",
            "1",
            "@countable",
            "REDUCE",
            "COUNT",
            "0",
        ]
        raw_result = await self.model.db().execute_command(*args)
        # RESP3 returns a dict ``{"results": [{...}, ...]}`` while RESP2
        # returns a flat ``[count, row1, row2, ...]`` list.  Each row's
        # COUNT value sits at index 3 of the legacy flat-pair row (the second
        # ``(name, value)`` pair) and inside ``extra_attributes`` of the RESP3
        # dict row, keyed by the generated alias ``__generated_aliascount``.
        rows: List[Any] = []
        if isinstance(raw_result, dict):
            rows = list(raw_result.get("results") or [])
        elif isinstance(raw_result, (list, tuple)):
            rows = list(raw_result[1:])
        try:
            counts = []
            for result in rows:
                if isinstance(result, dict):
                    extra = result.get("extra_attributes") or {}
                    if isinstance(extra, dict) and "__generated_aliascount" in extra:
                        counts.append(extra["__generated_aliascount"])
                        continue
                counts.append(result[3])
            return sum(
                [
                    int(
                        value.decode("utf-8", "ignore")
                        if isinstance(value, bytes)
                        else value
                    )
                    for value in counts
                ]
            )
        except (IndexError, TypeError, ValueError):
            return 0

    async def iter_cursor(
        self, count: int = DEFAULT_PAGE_SIZE, max_idle: Optional[int] = None
    ) -> FindQueryCursor:
        """Create a RediSearch cursor for this query.

        ``count`` controls the page size returned by each cursor read.
        ``max_idle`` is passed to RediSearch as WITHCURSOR MAXIDLE and is
        measured in milliseconds.
        """
        if count < 1:
            raise ValueError("Cursor count must be greater than zero.")
        if not await has_redisearch(self.model.db()):
            raise RedisModelError(
                "Your Redis instance does not have either the RediSearch module "
                "or RedisJSON module installed. Querying requires that your Redis "
                "instance has one of these modules installed."
            )
        if not getattr(self.model._meta, "index_health_checked", False):
            await self.model.check_index_health()
            self.model._meta.index_health_checked = True

        index_name = str(self.model.Meta.index_name)
        args: List[Union[str, bytes]] = [
            "FT.AGGREGATE",
            index_name,
            self.query,
            "LOAD",
            "1",
            "__key",
        ]
        if self.sort_fields:
            args += self.resolve_redisearch_aggregate_sort_fields()
        if self.query_params:
            args += ["PARAMS", str(len(self.query_params))] + self.query_params
        if self.knn:
            args += ["DIALECT", "2"]
        args += ["WITHCURSOR", "COUNT", str(count)]
        if max_idle is not None:
            args += ["MAXIDLE", str(max_idle)]

        raw_result = await self.model.db().execute_command(*args)
        protocol = protocol_version(self.model.db())
        aggregate_result, cursor_id = FindQueryCursor._split_cursor_result(
            raw_result, protocol=protocol
        )
        results = await FindQueryCursor._models_from_aggregate_result(
            self.model, aggregate_result
        )
        # ``_aggregate_total`` needs the inner aggregate payload (RESP3 dict
        # or RESP2 ``[count, key, fields, ...]``), not the WITHCURSOR wrapper.
        total_payload = raw_result
        if (
            isinstance(raw_result, (list, tuple))
            and len(raw_result) == 2
            and not isinstance(raw_result[0], (int, str, bytes))
        ):
            total_payload = raw_result[0]
        return FindQueryCursor(
            model=self.model,
            index_name=index_name,
            cursor_id=cursor_id,
            count=count,
            results=results,
            total=FindQueryCursor._aggregate_total(total_payload, protocol=protocol),
        )

    async def all(self, batch_size=DEFAULT_PAGE_SIZE):
        if batch_size != self.page_size:
            query = self.copy(page_size=batch_size, limit=batch_size)
            return await query.execute()
        return await self.execute()

    async def page(self, offset=0, limit=10):
        return await self.copy(offset=offset, limit=limit).execute(
            exhaust_results=False
        )

    def sort_by(self, *fields: str):
        if not fields:
            return self
        return self.copy(sort_fields=list(fields))

    async def update(self, use_transaction=True, **field_values):
        """
        Update models that match this query to the given field-value pairs.

        Keys and values given as keyword arguments are interpreted as fields
        on the target model and the values as the values to which to set the
        given fields.

        Limitation: all matching models are loaded into memory via
        ``self.all()`` before being saved back. For very large result sets
        consider chunking with ``self.page(offset, limit)`` and updating each
        page. Using ``async for`` over ``iter_cursor()`` would also work but is
        out of scope for this synchronous-feeling helper.
        """
        validate_model_fields(self.model, field_values)
        pipeline = await self.model.db().pipeline() if use_transaction else None

        for model in await self.all():
            for field, value in field_values.items():
                setattr(model, field, value)
            # When ``pipeline`` is None (non-transaction mode) redis-py returns
            # per-command responses from ``save()``, which we discard here. We
            # rely on each command raising on hard failures; soft errors are
            # surfaced only when ``use_transaction=True`` via ``pipeline.execute()``.
            await model.save(pipeline=pipeline)

        if pipeline:
            # ``pipeline.execute()`` returns a list of per-command responses.
            # We don't return them because their shape varies by command
            # (HSET vs JSON.SET) and is not part of the documented contract.
            # Errors inside the transaction surface as exceptions from
            # ``execute()`` itself.
            await pipeline.execute()

    async def delete(self):
        """Delete all matching records in this query.

        Returns the integer count of deleted keys (Redis ``DEL`` response).
        A ``ResponseError`` is swallowed and reported as ``0`` to keep the
        method total: callers that need stricter error handling should issue
        raw ``DEL`` commands against the model's database.
        """
        try:
            return await self.model.db().delete(*[m.key() for m in await self.all()])
        except ResponseError:
            return 0

    async def __aiter__(self):
        if self._model_cache:
            for m in self._model_cache:
                yield m
        else:
            for m in await self.execute():
                yield m

    def __getitem__(self, item: int):
        """
        Given this code:
            Model.find()[1000]

        We should return only the 1000th result.

            1. If the result is loaded in the query cache for this query,
               we can return it directly from the cache.

            2. If the query cache does not have enough elements to return
               that result, then we should clone the current query and
               give it a new offset and limit: offset=n, limit=1.
        """
        if ASYNC_MODE:
            raise QuerySyntaxError(
                "Cannot use [] notation with async code. "
                "Use FindQuery.get_item() instead."
            )
        if self._model_cache and len(self._model_cache) > item:
            return self._model_cache[item]

        query = self.copy(offset=item, limit=1)

        return query.execute()[0]  # noqa

    async def get_item(self, item: int):
        """
        Given this code:
            await Model.find().get_item(1000)

        We should return only the 1000th result.

            1. If the result is loaded in the query cache for this query,
               we can return it directly from the cache.

            2. If the query cache does not have enough elements to return
               that result, then we should clone the current query and
               give it a new offset and limit: offset=n, limit=1.

        NOTE: This method is included specifically for async users, who
        cannot use the notation Model.find()[1000].
        """
        if self._model_cache and len(self._model_cache) > item:
            return self._model_cache[item]

        query = self.copy(offset=item, limit=1)
        result = await query.execute()
        return result[0]


class PrimaryKeyCreator(Protocol):
    def create_pk(self, *args, **kwargs) -> str:
        """Create a new primary key"""


class UlidPrimaryKey:
    """
    A client-side generated primary key that follows the ULID spec.
    https://github.com/ulid/javascript#specification
    """

    @staticmethod
    def create_pk(*args, **kwargs) -> str:
        return str(ULID())


def __dataclass_transform__(
    *,
    eq_default: bool = True,
    order_default: bool = False,
    kw_only_default: bool = False,
    field_descriptors: Tuple[Union[type, Callable[..., Any]], ...] = (()),
) -> Callable[[_T], _T]:
    return lambda a: a


class FieldInfo(PydanticFieldInfo):  # type: ignore[misc]  # ty: ignore[subclass-of-final-class]
    def __init__(self, default: Any = Undefined, **kwargs: Any) -> None:
        primary_key = kwargs.pop("primary_key", False)
        sortable = kwargs.pop("sortable", Undefined)
        case_sensitive = kwargs.pop("case_sensitive", Undefined)
        index = kwargs.pop("index", Undefined)
        full_text_search = kwargs.pop("full_text_search", Undefined)
        vector_options = kwargs.pop("vector_options", None)
        separator = kwargs.pop("separator", SINGLE_VALUE_TAG_FIELD_SEPARATOR)
        if primary_key and index is Undefined:
            index = True
        super().__init__(default=default, **kwargs)
        self.primary_key = primary_key
        self.sortable = sortable
        self.case_sensitive = case_sensitive
        self.index = index
        self.full_text_search = full_text_search
        self.vector_options = vector_options
        self.separator = separator
        # Pydantic v2 merges Annotated metadata from its internal
        # _attributes_set, so mark Redis OM metadata as explicit when that
        # private attribute exists. If Pydantic changes or removes
        # _attributes_set, json_schema_extra still remains set on this
        # FieldInfo for normal (non-Annotated) fields.
        #
        # Upstream tracking: Pydantic is considering a public metadata-merging
        # API. When it lands, this private-attribute hook can be removed in
        # favor of the public API. See
        # https://docs.pydantic.dev/latest/concepts/fields/ for current field
        # metadata behavior.
        if hasattr(self, "_attributes_set"):
            self._attributes_set["json_schema_extra"] = self.json_schema_extra


REDIS_OM_FIELD_DEFAULTS = {
    "primary_key": False,
    "sortable": False,
    "case_sensitive": False,
    "index": False,
    "full_text_search": False,
    "vector_options": None,
    "separator": SINGLE_VALUE_TAG_FIELD_SEPARATOR,
}
REDIS_OM_METADATA_KEY = "redis_om"
# Redis OM stores custom field metadata in Pydantic v2's json_schema_extra so
# inherited/copied FieldInfo instances retain ORM-specific options.


def _get_redis_om_metadata(field: Any) -> Dict[str, Any]:
    extra = getattr(field, "json_schema_extra", None) or {}
    return dict(extra.get(REDIS_OM_METADATA_KEY, {}))


def _get_redis_om_field_attr(field: Any, attr: str, default: Any = None) -> Any:
    extra = _get_redis_om_metadata(field)
    return extra.get(attr, default)


def _set_redis_om_field_attr(field: Any, attr: str, value: Any) -> None:
    # Pydantic v2 uses the PydanticUndefined sentinel to signal "unset."  Storing
    # it in json_schema_extra would leak a non-JSON-serializable value into the
    # field metadata, which breaks schema generation (e.g. FastAPI's
    # /openapi.json).  The getter falls back to the field default when the attr
    # is absent, so skipping the sentinel is safe.
    if value is Undefined:
        return
    extra = dict(getattr(field, "json_schema_extra", None) or {})
    metadata = dict(extra.get(REDIS_OM_METADATA_KEY, {}))
    metadata[attr] = value
    extra[REDIS_OM_METADATA_KEY] = metadata
    field.json_schema_extra = extra


def _redis_om_field_property(attr: str, default: Any) -> property:
    def getter(self):
        return _get_redis_om_field_attr(self, attr, default)

    def setter(self, value):
        _set_redis_om_field_attr(self, attr, value)

    return property(getter, setter)


for _attr_name, _attr_default in REDIS_OM_FIELD_DEFAULTS.items():
    setattr(
        PydanticFieldInfo,
        _attr_name,
        _redis_om_field_property(_attr_name, _attr_default),
    )


def _apply_redis_om_field_metadata(target: Any, source: Optional[Any] = None) -> Any:
    source = source or target
    for attr, default in REDIS_OM_FIELD_DEFAULTS.items():
        value = getattr(source, attr, Undefined)
        if value is not Undefined:
            setattr(target, attr, value)
        elif not hasattr(target, attr):
            setattr(target, attr, default)
    return target


def should_index_field(field_info: Any) -> bool:
    """Determine whether a field should be added to the RediSearch index.

    A field is indexed if any of the following are true:
      * ``index=True``
      * the field has ``vector_options`` set
      * the field is marked ``full_text_search=True``
      * the field is marked ``sortable=True``

    Vector, full-text-search, and sortable fields must always be indexed for
    RediSearch to support those features, so we index them even when
    ``index`` is not explicitly set.
    """
    _index = getattr(field_info, "index", None)

    index = _index is True
    vector_options = getattr(field_info, "vector_options", None) is not None
    full_text_search = getattr(field_info, "full_text_search", None) is True
    sortable = getattr(field_info, "sortable", None) is True

    return index or vector_options or full_text_search or sortable


class RelationshipInfo(Representation):
    def __init__(
        self,
        *,
        back_populates: Optional[str] = None,
        link_model: Optional[Any] = None,
    ) -> None:
        self.back_populates = back_populates
        self.link_model = link_model


@dataclasses.dataclass
class VectorFieldOptions:
    class ALGORITHM(Enum):
        FLAT = "FLAT"
        HNSW = "HNSW"

    class TYPE(Enum):
        FLOAT32 = "FLOAT32"
        FLOAT64 = "FLOAT64"

    class DISTANCE_METRIC(Enum):
        L2 = "L2"
        IP = "IP"
        COSINE = "COSINE"

    algorithm: ALGORITHM
    type: TYPE
    dimension: int
    distance_metric: DISTANCE_METRIC

    # Common optional parameters
    initial_cap: Optional[int] = None

    # Optional parameters for FLAT
    block_size: Optional[int] = None

    # Optional parameters for HNSW
    m: Optional[int] = None
    ef_construction: Optional[int] = None
    ef_runtime: Optional[int] = None
    epsilon: Optional[float] = None

    @staticmethod
    def flat(
        type: TYPE,
        dimension: int,
        distance_metric: DISTANCE_METRIC,
        initial_cap: Optional[int] = None,
        block_size: Optional[int] = None,
    ):
        return VectorFieldOptions(
            algorithm=VectorFieldOptions.ALGORITHM.FLAT,
            type=type,
            dimension=dimension,
            distance_metric=distance_metric,
            initial_cap=initial_cap,
            block_size=block_size,
        )

    @staticmethod
    def hnsw(
        type: TYPE,
        dimension: int,
        distance_metric: DISTANCE_METRIC,
        initial_cap: Optional[int] = None,
        m: Optional[int] = None,
        ef_construction: Optional[int] = None,
        ef_runtime: Optional[int] = None,
        epsilon: Optional[float] = None,
    ):
        return VectorFieldOptions(
            algorithm=VectorFieldOptions.ALGORITHM.HNSW,
            type=type,
            dimension=dimension,
            distance_metric=distance_metric,
            initial_cap=initial_cap,
            m=m,
            ef_construction=ef_construction,
            ef_runtime=ef_runtime,
            epsilon=epsilon,
        )

    @property
    def schema(self):
        attr = []
        for k, v in vars(self).items():
            if k == "algorithm" or v is None:
                continue
            attr.extend(
                [
                    k.upper() if k != "dimension" else "DIM",
                    str(v) if not isinstance(v, Enum) else v.name,
                ]
            )

        return " ".join([f"VECTOR {self.algorithm.name} {len(attr)}"] + attr)


def Field(
    default: Any = Undefined,
    *,
    default_factory: Optional[NoArgAnyCallable] = None,
    alias: Optional[str] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
    exclude: Union[
        AbstractSet[Union[int, str]], Mapping[Union[int, str], Any], Any
    ] = None,
    include: Union[
        AbstractSet[Union[int, str]], Mapping[Union[int, str], Any], Any
    ] = None,
    const: Optional[bool] = None,
    gt: Optional[float] = None,
    ge: Optional[float] = None,
    lt: Optional[float] = None,
    le: Optional[float] = None,
    multiple_of: Optional[float] = None,
    min_items: Optional[int] = None,
    max_items: Optional[int] = None,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None,
    allow_mutation: bool = True,
    regex: Optional[str] = None,
    primary_key: bool = False,
    sortable: Union[bool, UndefinedType] = Undefined,
    case_sensitive: Union[bool, UndefinedType] = Undefined,
    index: Union[bool, UndefinedType] = Undefined,
    full_text_search: Union[bool, UndefinedType] = Undefined,
    vector_options: Optional[VectorFieldOptions] = None,
    separator: str = SINGLE_VALUE_TAG_FIELD_SEPARATOR,
    schema_extra: Optional[Dict[str, Any]] = None,
) -> Any:
    current_schema_extra = schema_extra or {}
    field_info = FieldInfo(
        default,
        default_factory=default_factory,
        alias=alias,
        title=title,
        description=description,
        exclude=exclude,
        include=include,
        const=const,
        gt=gt,
        ge=ge,
        lt=lt,
        le=le,
        multiple_of=multiple_of,
        min_items=min_items,
        max_items=max_items,
        min_length=min_length,
        max_length=max_length,
        allow_mutation=allow_mutation,
        regex=regex,
        primary_key=primary_key,
        sortable=sortable,
        case_sensitive=case_sensitive,
        index=index,
        full_text_search=full_text_search,
        vector_options=vector_options,
        separator=separator,
        **current_schema_extra,
    )
    return field_info


@dataclasses.dataclass
class PrimaryKey:
    name: str
    field: ModelField


class BaseMeta(Protocol):
    global_key_prefix: str
    model_key_prefix: str
    primary_key_pattern: str
    database: Optional[Union[DatabaseConnection, DatabaseProvider]]
    primary_key: PrimaryKey
    primary_key_creator_cls: Type[PrimaryKeyCreator]
    index_name: str
    embedded: bool
    encoding: str
    default_ttl: Optional[int]
    index_health_checked: bool
    # Bookkeeping for lazy database resolution; not part of the public API.
    _database_generated: bool
    _database_loop: Optional[asyncio.AbstractEventLoop]


@dataclasses.dataclass
class DefaultMeta:
    """A default placeholder Meta object.

    This is necessary: every model class needs a Meta object to read settings
    from, and ``ModelMeta.__new__`` populates it with sane defaults if the user
    doesn't define their own ``Meta`` class. Making the fields optional here
    lets us distinguish "explicitly set to None/falsy" from "never set," which
    matters for inheritance through ``getattr(base_meta, ...)``.
    """

    global_key_prefix: Optional[str] = None
    model_key_prefix: Optional[str] = None
    primary_key_pattern: Optional[str] = None
    # Separator used to join key-prefix segments in the default ``index_name``.
    # Defaults to ``":"", matching historical Redis key conventions. Override
    # per model via ``class Meta`` to customize the index name format without
    # forking the library.
    key_separator: str = ":"
    # May be a connection instance or a callable that returns one lazily.
    database: Optional[Union[DatabaseConnection, DatabaseProvider]] = None
    primary_key: Optional[PrimaryKey] = None
    primary_key_creator_cls: Optional[Type[PrimaryKeyCreator]] = None
    index_name: Optional[str] = None
    embedded: Optional[bool] = False
    encoding: str = "utf-8"
    default_ttl: Optional[int] = None
    index_health_checked: bool = False
    _database_generated: bool = False
    _database_loop: Optional[asyncio.AbstractEventLoop] = None


class ModelMeta(ModelMetaclass):
    _meta: BaseMeta

    def __new__(cls, name, bases, attrs, **kwargs):  # noqa C901
        meta = attrs.pop("Meta", None)
        new_class: Any = super().__new__(cls, name, bases, attrs, **kwargs)
        # `super().__new__` is typed as returning `type`, but this class is
        # constructed by `ModelMeta`, which attaches `_meta`, `Meta`, and
        # pydantic internals dynamically. The local annotation above widens
        # it to `Any` so attribute access inside this method type-checks.

        # The fact that there is a Meta field and _meta field is important: a
        # user may have given us a Meta object with their configuration, while
        # we might have inherited _meta from a parent class, and should
        # therefore use some of the inherited fields.
        meta = meta or getattr(new_class, "Meta", None)
        base_meta = getattr(new_class, "_meta", None)

        if meta and meta != DefaultMeta and meta != base_meta:
            new_class.Meta = meta
            new_class._meta = meta
        elif base_meta:
            new_class._meta = type(
                f"{new_class.__name__}Meta", (base_meta,), dict(base_meta.__dict__)
            )
            new_class.Meta = new_class._meta
            # Unset inherited values we don't want to reuse (typically based on
            # the model name).
            new_class._meta.model_key_prefix = None
            new_class._meta.index_name = None
        else:
            new_class._meta = type(
                f"{new_class.__name__}Meta", (DefaultMeta,), dict(DefaultMeta.__dict__)
            )
            new_class.Meta = new_class._meta

        # Create proxies for each model field so that we can use the field
        # in queries, like Model.get(Model.field_name == 1)
        for field_name, field in new_class.model_fields.items():
            inherited_field = None
            for base_candidate in bases:
                inherited_field = getattr(base_candidate, "model_fields", {}).get(
                    field_name
                )
                if inherited_field is not None:
                    break
            _apply_redis_om_field_metadata(field, inherited_field or field)
            model_field = ModelField(field_info=field, name=field_name)
            # Embedded models should never get an ExpressionProxy for the
            # inherited pk field — it is not a real field on embedded models.
            # We must also explicitly shadow the parent pk with None so that
            # attribute lookup never falls through to the parent class's
            # ExpressionProxy (which Pydantic v2 would try to validate).
            is_embedded_pk = (
                getattr(new_class._meta, "embedded", False) and field_name == "pk"
            )
            if is_embedded_pk:
                setattr(new_class, field_name, None)
                if field_name in new_class.__annotations__:
                    new_class.__annotations__[field_name] = field.annotation
                # Pydantic v2 bakes the class attribute into the core schema as
                # the field default.  We need to clear it from the FieldInfo and
                # rebuild so that validation doesn't receive the ExpressionProxy.
                field.default = None
                continue

            setattr(new_class, field_name, ExpressionProxy(model_field, []))
            annotation = new_class.get_annotations().get(field_name)
            if annotation:
                new_class.__annotations__[field_name] = Union[
                    annotation, ExpressionProxy
                ]
            else:
                new_class.__annotations__[field_name] = ExpressionProxy
            if field.primary_key:
                new_class._meta.primary_key = PrimaryKey(
                    name=field_name, field=model_field
                )
            if field.vector_options:
                score_attr = f"_{field_name}_score"
                setattr(new_class, score_attr, None)
                new_class.__annotations__[score_attr] = Union[float, None]

        # Pydantic v2 copies the class-level attribute into the core schema as
        # the field default.  Because ``setattr(new_class, field_name, ExpressionProxy)``
        # runs *after* ``super().__new__()``, the core schema already contains the
        # parent class's ExpressionProxy as the default for ``pk`` (and any other
        # inherited field).  We must rebuild the schema so that Pydantic reads the
        # newly-set class attribute instead of the stale inherited one.
        #
        # Additionally, for ``pk`` specifically, we need to clear the
        # ``ExpressionProxy`` from the ``FieldInfo`` default so that
        # ``model_validate`` and ``model_validate_json`` don't try to validate
        # it as a string.
        pk_field = new_class.model_fields.get("pk")
        if pk_field is not None and getattr(pk_field, "default", None) is not None:
            pk_field.default = None
        new_class.model_rebuild(force=True)

        # If this is an embedded model, we don't want to allow primary keys at all,
        if getattr(new_class._meta, "embedded", False):
            new_class._meta.primary_key = None

        if not getattr(new_class._meta, "global_key_prefix", None):
            new_class._meta.global_key_prefix = getattr(
                base_meta, "global_key_prefix", ""
            )
        if not getattr(new_class._meta, "model_key_prefix", None):
            # Don't look at the base class for this.
            new_class._meta.model_key_prefix = (
                f"{new_class.__module__}.{new_class.__name__}"
            )
        if not getattr(new_class._meta, "primary_key_pattern", None):
            new_class._meta.primary_key_pattern = getattr(
                base_meta, "primary_key_pattern", "{pk}"
            )
        if not getattr(new_class._meta, "database", None):
            new_class._meta.database = getattr(base_meta, "database", None)
        if not getattr(new_class._meta, "encoding", None):
            new_class._meta.encoding = getattr(base_meta, "encoding")
        if not getattr(new_class._meta, "primary_key_creator_cls", None):
            new_class._meta.primary_key_creator_cls = getattr(
                base_meta, "primary_key_creator_cls", UlidPrimaryKey
            )
        if getattr(new_class._meta, "default_ttl", None) is None:
            new_class._meta.default_ttl = getattr(base_meta, "default_ttl", None)
        new_class._meta.index_health_checked = False
        # Resolve the key separator (defaults to ":"). It can be customized per
        # model via ``class Meta: key_separator = ...``. We read it from
        # ``base_meta`` first so subclasses inherit the parent's choice.
        if not getattr(new_class._meta, "key_separator", None):
            new_class._meta.key_separator = getattr(base_meta, "key_separator", ":")
        separator = new_class._meta.key_separator
        if not getattr(new_class._meta, "index_name", None):
            new_class._meta.index_name = (
                f"{new_class._meta.global_key_prefix}{separator}"
                f"{new_class._meta.model_key_prefix}{separator}index"
            )

        if (
            name != "RedisModel"
            and abc.ABC not in bases
            and hasattr(new_class, "redisearch_schema")
        ):
            new_class.redisearch_schema()

        # Not an abstract model class or embedded model, so we should let the
        # Migrator create indexes for it.
        if abc.ABC not in bases and not getattr(new_class._meta, "embedded", False):
            key = f"{new_class.__module__}.{new_class.__qualname__}"
            model_registry[key] = new_class

        return new_class


def outer_type_or_annotation(field):
    if hasattr(field, "outer_type_"):
        return field.outer_type_
    annotation = _unwrap_type_annotation(field.annotation)
    origin = get_origin(annotation)
    if origin == Literal:
        return annotation
    if isinstance(annotation, type) or origin is not None:
        return annotation
    if hasattr(annotation, "__args__") and annotation.__args__:
        return annotation.__args__[0]
    raise AttributeError(f"could not extract outer type from field {field}")


class RedisModel(BaseModel, abc.ABC, metaclass=ModelMeta):
    pk: Optional[str] = Field(default=None, primary_key=True)

    Meta = DefaultMeta
    # Populated by ``ModelMeta.__new__``; declared here so attribute access on
    # instances and subclasses resolves to ``BaseMeta`` for static analysis.
    _meta: ClassVar[BaseMeta]

    model_config: ClassVar[ConfigDict] = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True,
        extra="allow",
    )

    @model_validator(mode="before")
    @classmethod
    def _strip_expression_proxy_pk(cls, data: Any) -> Any:
        """Strip ExpressionProxy values from pk before Pydantic validates.

        Framework integrations (e.g. strawberry-graphql) may pass the
        class-level ``Model.pk`` attribute — which is an ExpressionProxy
        used for query building — as the ``pk`` value in input data.
        Because strawberry defers Pydantic validation until ``to_pydantic()``
        is called, the ExpressionProxy reaches ``model_validate()`` and causes
        a ``ValidationError``.

        We strip it here so that ``model_validate()`` treats it as if pk
        was omitted, allowing ``model_post_init`` to generate a proper pk.
        """
        if isinstance(data, dict) and "pk" in data:
            if isinstance(data["pk"], ExpressionProxy):
                data = {k: v for k, v in data.items() if k != "pk"}
        return data

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__()

    def __init__(__pydantic_self__, **data: Any) -> None:
        __pydantic_self__.validate_primary_key()
        super().__init__(**data)

    def model_post_init(self, __context: Any) -> None:
        if getattr(type(self)._meta, "embedded", False):
            # Always clear pk for embedded models — they don't have their own pk.
            if isinstance(self.pk, ExpressionProxy):
                object.__setattr__(self, "pk", None)
        elif not self.pk or isinstance(self.pk, ExpressionProxy):
            object.__setattr__(
                self, "pk", type(self)._meta.primary_key_creator_cls().create_pk()
            )

    def _model_dump_exclude(self, exclude: Any) -> Any:
        is_embedded = getattr(type(self)._meta, "embedded", False)
        if is_embedded and (self.pk is None or isinstance(self.pk, ExpressionProxy)):
            if exclude is None:
                exclude = {"pk"}
            elif isinstance(exclude, AbstractSet):
                exclude = set(exclude)
                exclude.add("pk")
            elif isinstance(exclude, dict):
                exclude = dict(exclude)
                exclude["pk"] = True
        return exclude

    def model_dump(self, *args, **kwargs):
        exclude = self._model_dump_exclude(kwargs.pop("exclude", None))
        data = super().model_dump(*args, exclude=exclude, **kwargs)
        return strip_null_embedded_pks(type(self), data)

    def dict(self, *args, **kwargs):
        """Backwards-compatible wrapper over Pydantic v2's model_dump()."""
        return self.model_dump(*args, **kwargs)

    def __lt__(self, other):
        """Default sort: compare primary key of models."""
        return self.key() < other.key()

    def key(self):
        """Return the Redis key for this model."""
        if hasattr(self._meta.primary_key.field, "name"):
            pk = getattr(self, self._meta.primary_key.field.name)
        else:
            pk = getattr(self, self._meta.primary_key.name)
        return self.make_primary_key(pk)

    @classmethod
    async def _delete(cls, db, *pks):
        return await db.delete(*pks)

    @classmethod
    async def delete(
        cls, pk: Any, pipeline: Optional[redis.client.Pipeline] = None
    ) -> int:
        """Delete data at this key."""
        db = cls._get_db(pipeline)

        return await cls._delete(db, cls.make_primary_key(pk))

    @classmethod
    async def get(cls: Type["Model"], pk: Any) -> "Model":
        raise NotImplementedError

    async def update(self, **field_values):
        """Update this model instance with the specified key-value pairs."""
        raise NotImplementedError

    async def save(
        self: "Model", pipeline: Optional[redis.client.Pipeline] = None
    ) -> "Model":
        raise NotImplementedError

    async def expire(
        self, num_seconds: int, pipeline: Optional[redis.client.Pipeline] = None
    ):
        db = self._get_db(pipeline)

        # ClusterPipeline commands must not be awaited; doing so consumes the
        # response instead of queuing the command for batch execution.
        if _is_cluster_pipeline(db):
            db.expire(self.key(), num_seconds)
        else:
            await db.expire(self.key(), num_seconds)

    @classmethod
    def validate_primary_key(cls):
        """Check for a primary key. We need one (and only one)."""
        if getattr(cls._meta, "embedded", False):
            return
        primary_keys = 0
        for name, field_info in cls.model_fields.items():
            if getattr(field_info, "primary_key", None):
                primary_keys += 1
        if primary_keys == 0:
            raise RedisModelError("You must define a primary key for the model")
        elif primary_keys > 2:
            raise RedisModelError("You must define only one primary key for a model")

    @classmethod
    def make_key(cls, part: str):
        global_prefix = getattr(cls._meta, "global_key_prefix", "").strip(":")
        model_prefix = getattr(cls._meta, "model_key_prefix", "").strip(":")
        return f"{global_prefix}:{model_prefix}:{part}"

    @classmethod
    def make_primary_key(cls, pk: Any):
        """Return the Redis key for this model."""
        return cls.make_key(cls._meta.primary_key_pattern.format(pk=pk))

    @classmethod
    def db(cls):
        # `Meta` is the user-facing configuration surface; `_meta` is the
        # internal copy inherited and normalized by the metaclass.
        database = getattr(cls.Meta, "database", None)
        if database is None:
            database = getattr(cls._meta, "database", None)
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None
        if callable(database):
            database = database()
            cls.Meta.database = database
            cls._meta.database = database
            cls.Meta._database_generated = False
            cls._meta._database_generated = False
            cls.Meta._database_loop = None
            cls._meta._database_loop = None
        elif (
            database is not None
            and getattr(cls.Meta, "_database_generated", False)
            and current_loop is not None
            and getattr(cls.Meta, "_database_loop", None) is not current_loop
        ):
            database = None
        if database is None:
            database = get_redis_connection()
            cls.Meta.database = database
            cls._meta.database = database
            cls.Meta._database_generated = True
            cls._meta._database_generated = True
            cls.Meta._database_loop = current_loop
            cls._meta._database_loop = current_loop
        return database

    @classmethod
    def default_ttl(cls) -> Optional[int]:
        default_ttl = getattr(cls.Meta, "default_ttl", None)
        if default_ttl is None:
            default_ttl = getattr(cls._meta, "default_ttl", None)
        return default_ttl

    @classmethod
    def save_response_count(cls) -> int:
        # save + expire when a default TTL is configured, otherwise save only.
        return 2 if cls.default_ttl() is not None else 1

    async def finalize_save(
        self, pipeline: Optional[redis.client.Pipeline] = None
    ) -> None:
        await self.apply_default_ttl(pipeline=pipeline)
        # Re-check index health on the next query after writes.
        type(self)._meta.index_health_checked = False

    async def apply_default_ttl(
        self, pipeline: Optional[redis.client.Pipeline] = None
    ) -> None:
        default_ttl = self.default_ttl()
        if default_ttl is None:
            return
        await self.expire(default_ttl, pipeline=pipeline)

    @staticmethod
    def _normalize_redis_info(value: Any) -> Any:
        if isinstance(value, bytes):
            return value.decode("utf-8", "ignore")
        if isinstance(value, dict):
            return {
                RedisModel._normalize_redis_info(key): RedisModel._normalize_redis_info(
                    dict_value
                )
                for key, dict_value in value.items()
            }
        if isinstance(value, list):
            return [RedisModel._normalize_redis_info(item) for item in value]
        return value

    @classmethod
    def _find_first_nonempty_value(
        cls, *mappings: Mapping[str, Any], keys: Tuple[str, ...]
    ) -> Optional[Any]:
        """Return the first non-empty value found for any key in the mappings."""
        for mapping in mappings:
            if not isinstance(mapping, dict):
                continue
            for key in keys:
                value = mapping.get(key)
                if value not in (None, "", [], {}):
                    return value
        return None

    @staticmethod
    def _stringify_redis_info_value(value: Any) -> str:
        """Convert Redis INFO values into stable strings for warning messages."""
        if isinstance(value, (dict, list)):
            return json.dumps(value, sort_keys=True)
        return str(value)

    @classmethod
    async def check_index_health(cls) -> Optional[Dict[str, Any]]:
        try:
            index_info = await cls.db().ft(cls.Meta.index_name).info()
        except (AttributeError, redis.ResponseError):
            return None

        info = cls._normalize_redis_info(index_info)
        index_errors = info.get("Index Errors") or info.get("index_errors") or {}
        # RediSearch response shapes vary by version, so we check the common
        # top-level and nested key variants for indexing failures.
        indexing_failures = 0
        for mapping, key in (
            (info, "hash_indexing_failures"),
            (index_errors, "hash_indexing_failures"),
            (index_errors, "indexing failures"),
            (info, "indexing failures"),
        ):
            if isinstance(mapping, dict):
                value = mapping.get(key)
                if value is not None:
                    indexing_failures = value
                    break

        try:
            indexing_failures = int(indexing_failures)
        except (TypeError, ValueError):
            indexing_failures = 0

        last_indexing_error = cls._find_first_nonempty_value(
            index_errors,
            info,
            keys=(
                "last indexing error",
                "last_indexing_error",
                "last error",
                "last_error",
            ),
        )
        last_indexing_error_key = cls._find_first_nonempty_value(
            index_errors,
            info,
            keys=(
                "last indexing error key",
                "last_indexing_error_key",
                "last error key",
                "last_error_key",
            ),
        )
        health = {
            "index_name": cls.Meta.index_name,
            "indexing_failures": indexing_failures,
            "index_errors": index_errors,
            "last_indexing_error": last_indexing_error,
            "last_indexing_error_key": last_indexing_error_key,
        }
        if indexing_failures:
            detail_parts = []
            if last_indexing_error is not None:
                detail_parts.append(
                    "Last indexing error: "
                    + cls._stringify_redis_info_value(last_indexing_error)
                )
            if last_indexing_error_key is not None:
                detail_parts.append(
                    "Key: " + cls._stringify_redis_info_value(last_indexing_error_key)
                )
            detail_suffix = f" {'; '.join(detail_parts)}." if detail_parts else ""
            log.warning(
                "RediSearch index %s for %s reports %s indexing failures. "
                "Queries may return incomplete results.%sRun FT.INFO %s for details.",
                cls.Meta.index_name,
                cls.__name__,
                indexing_failures,
                detail_suffix,
                cls.Meta.index_name,
            )
        return health

    @classmethod
    def find(
        cls,
        *expressions: Union[Any, Expression],
        knn: Optional[KNNExpression] = None,
    ) -> FindQuery:
        return FindQuery(expressions=expressions, knn=knn, model=cls)

    @classmethod
    def from_redis(cls, res: Any, protocol: Optional[int] = None):
        # ``res`` may come from a RESP2 wire (flat list) or a RESP3 wire
        # (structured dict).  ``is_resp3_search_response`` accepts both ``str``
        # and ``bytes`` dict keys because redis-py does not always decode
        # RESP3 map keys for raw ``execute_command`` callers.
        def to_string(s):
            if isinstance(s, (str,)):
                return s
            elif isinstance(s, bytes):
                return s.decode(errors="ignore")
            else:
                return s  # Not a string we care about

        if isinstance(res, dict) and (protocol == 3 or is_resp3_search_response(res)):
            # RESP3 path: structured dict from FT.SEARCH.  Walk the ``results``
            # entries and reuse the same fields-handling logic as the RESP2
            # path.  Normalise bytes keys to ``str`` up front so the rest of
            # this branch (and the helpers it calls) can use plain string
            # comparisons.
            res = _decode_dict_keys(res)
            docs = []
            for entry in res.get("results") or []:
                if not isinstance(entry, dict):
                    continue
                entry = _decode_dict_keys(entry)
                extra = _decode_dict_keys(entry.get("extra_attributes") or {})
                # ``extra_attributes`` is already a flat dict of decoded
                # strings/values; convert to the ``[name, value, ...]`` shape
                # the rest of the method expects so legacy code paths work.
                fields_list: List[Any] = []
                for name, value in extra.items():
                    fields_list.append(name)
                    fields_list.append(value)
                # ``values`` carries LOAD-bearing fields (e.g. score fields).
                for v in entry.get("values") or []:
                    if isinstance(v, list) and len(v) == 2:
                        fields_list.append(v[0])
                        fields_list.append(v[1])
                if not fields_list:
                    continue
                fields: Dict[str, str] = dict(
                    zip(
                        map(to_string, fields_list[::2]),
                        map(to_string, fields_list[1::2]),
                    )
                )
                if fields.get("$"):
                    json_fields = json.loads(fields.pop("$"))
                    model_fields = get_model_fields(cls)
                    json_fields = convert_timestamp_to_datetime(
                        json_fields, model_fields
                    )
                    json_fields = convert_base64_to_bytes(json_fields, model_fields)
                    doc = cls(**json_fields)
                    for k, v in fields.items():
                        if k.startswith("__") and k.endswith("_score"):
                            setattr(doc, k[1:], float(v))
                        elif k.endswith("_score") and hasattr(doc, k):
                            setattr(doc, k, float(v))
                else:
                    model_fields = get_model_fields(cls)
                    fields = convert_empty_strings_to_none(fields, model_fields)
                    fields = convert_base64_to_bytes(fields, model_fields)
                    doc = cls(**fields)
                docs.append(doc)
            return docs

        # Legacy RESP2 path.  If we land here with a dict it means the caller
        # passed something we couldn't identify as a RESP3 search response;
        # rather than indexing into the dict and raising ``KeyError``, return
        # an empty result set (consistent with an unparseable response).
        if isinstance(res, dict):
            return []

        docs = []
        step = 2  # Because the result has content
        offset = 1  # The first item is the count of total matches.

        for i in range(1, len(res), step):
            if res[i + offset] is None:
                continue
            fields: Dict[str, str] = dict(
                zip(
                    map(to_string, res[i + offset][::2]),
                    map(to_string, res[i + offset][1::2]),
                )
            )
            # $ means a json entry
            if fields.get("$"):
                json_fields = json.loads(fields.pop("$"))
                model_fields = get_model_fields(cls)
                json_fields = convert_timestamp_to_datetime(json_fields, model_fields)
                json_fields = convert_base64_to_bytes(json_fields, model_fields)
                doc = cls(**json_fields)
                for k, v in fields.items():
                    if k.startswith("__") and k.endswith("_score"):
                        setattr(doc, k[1:], float(v))
                    elif k.endswith("_score") and hasattr(doc, k):
                        setattr(doc, k, float(v))
            else:
                model_fields = get_model_fields(cls)
                fields = convert_empty_strings_to_none(fields, model_fields)
                fields = convert_base64_to_bytes(fields, model_fields)
                doc = cls(**fields)

            docs.append(doc)
        return docs

    @classmethod
    def get_annotations(cls):
        d = {}
        for c in cls.mro():
            try:
                d.update(**c.__annotations__)
            except AttributeError:
                # object, at least, has no __annotations__ attribute.
                pass
        return d

    @classmethod
    async def add(
        cls: Type["Model"],
        models: Sequence["Model"],
        pipeline: Optional[redis.client.Pipeline] = None,
        pipeline_verifier: Callable[..., Any] = verify_pipeline_response,
    ) -> Sequence["Model"]:
        db = cls._get_db(pipeline, bulk=True)

        for model in models:
            # save() just returns the model, we don't need that here.
            await model.save(pipeline=db)

        # If the user didn't give us a pipeline, then we need to execute
        # the one we just created.
        if pipeline is None:
            result = await db.execute()
            pipeline_verifier(
                result, expected_responses=len(models) * cls.save_response_count()
            )

        return models

    @classmethod
    def _get_db(
        self, pipeline: Optional[redis.client.Pipeline] = None, bulk: bool = False
    ):
        if pipeline is not None:
            return pipeline
        elif bulk:
            return self.db().pipeline(transaction=False)
        else:
            return self.db()

    @classmethod
    async def delete_many(
        cls,
        models: Sequence["RedisModel"],
        pipeline: Optional[redis.client.Pipeline] = None,
    ) -> int:
        db = cls._get_db(pipeline)

        for chunk in ichunked(models, 100):
            pks = [model.key() for model in chunk]
            await cls._delete(db, *pks)

        return len(models)

    @classmethod
    async def get_many(
        cls: Type["Model"],
        pks: Sequence[Any],
        pipeline: Optional[redis.client.Pipeline] = None,
    ) -> Sequence["Model"]:
        """Retrieve multiple model instances by primary key using a pipeline.

        This minimises network overhead by fetching all records in a single
        round-trip instead of issuing one request per key.

        Args:
            pks: A sequence of primary key values.
            pipeline: An optional explicit Redis pipeline.  When *None* an
                implicit pipeline is created automatically.

        Returns:
            A list of model instances in the same order as *pks*.  Missing
            keys are silently skipped (no ``NotFoundError`` is raised for
            individual missing keys).
        """
        raise NotImplementedError

    @classmethod
    def redisearch_schema(cls):
        raise NotImplementedError

    def check(self):
        """Run all validations."""
        validate_model_data(self.__class__, self.__dict__)


class HashModel(RedisModel, abc.ABC):
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        if hasattr(cls, "__annotations__"):
            for name, field_type in cls.__annotations__.items():
                origin = get_origin(field_type)
                for typ in (Set, Mapping, List):
                    if isinstance(origin, type) and issubclass(origin, typ):  # type: ignore
                        raise RedisModelError(
                            f"HashModels cannot index set, list, "
                            f"or mapping fields. Field: {name}"
                        )
                if isinstance(field_type, type) and issubclass(field_type, RedisModel):
                    raise RedisModelError(
                        f"HashModels cannot index embedded model fields. Field: {name}"
                    )
                elif (
                    isinstance(field_type, type)
                    and dataclasses.is_dataclass(field_type)
                    and not issubclass(field_type, Coordinates)
                ):
                    raise RedisModelError(
                        f"HashModels cannot index dataclass fields. Field: {name}"
                    )

        for name, field in cls.model_fields.items():
            outer_type = outer_type_or_annotation(field)
            origin = get_origin(outer_type)
            if origin:
                for typ in (Set, Mapping, List):
                    if isinstance(origin, type) and issubclass(origin, typ):  # type: ignore
                        raise RedisModelError(
                            f"HashModels cannot index set, list, "
                            f"or mapping fields. Field: {name}"
                        )

            if isinstance(outer_type, type) and issubclass(outer_type, RedisModel):
                raise RedisModelError(
                    f"HashModels cannot index embedded model fields. Field: {name}"
                )
            elif (
                isinstance(outer_type, type)
                and dataclasses.is_dataclass(outer_type)
                and not issubclass(outer_type, Coordinates)
            ):
                raise RedisModelError(
                    f"HashModels cannot index dataclass fields. Field: {name}"
                )

    async def save(
        self: "Model", pipeline: Optional[redis.client.Pipeline] = None
    ) -> "Model":
        self.check()
        db = self._get_db(pipeline)

        # Get model data and convert datetime objects first
        document = self.model_dump()
        document = convert_datetime_to_timestamp(document)
        document = convert_bytes_to_base64(document)
        document = convert_dataclasses_to_dicts(document)

        # Then apply jsonable encoding for other types
        document = jsonable_encoder(document)

        # filter out values which are `None` because they are not valid in a HSET
        document = {k: v for k, v in document.items() if v is not None}
        # ClusterPipeline commands must not be awaited; doing so consumes the
        # response instead of queuing the command for batch execution.
        if _is_cluster_pipeline(db):
            db.hset(self.key(), mapping=document)
        else:
            await db.hset(self.key(), mapping=document)
        await self.finalize_save(pipeline=pipeline)
        return self

    @classmethod
    async def all_pks(cls, count: Optional[int] = None):
        key_prefix = cls.make_key(cls._meta.primary_key_pattern.format(pk=""))
        scan_kwargs: Dict[str, Any] = {"_type": "HASH"}
        if count is not None:
            scan_kwargs["count"] = count
        # SCAN keys are returned as ``bytes`` when ``decode_responses=False``
        # and ``str`` otherwise. We decode bytes using the model's encoding
        # (default ``utf-8``) before stripping the key prefix.
        return (
            (
                remove_prefix(key, key_prefix)
                if isinstance(key, str)
                else remove_prefix(key.decode(cls.Meta.encoding), key_prefix)
            )
            async for key in cls.db().scan_iter(f"{key_prefix}*", **scan_kwargs)
        )

    @classmethod
    async def get(cls: Type["Model"], pk: Any) -> "Model":
        document = await cls.db().hgetall(cls.make_primary_key(pk))
        if not document:
            raise NotFoundError
        # Convert empty strings back to None for Optional fields (fixes #254)
        model_fields = get_model_fields(cls)
        document = convert_empty_strings_to_none(document, model_fields)
        document = convert_base64_to_bytes(document, model_fields)
        document = restore_missing_pk(cls, document, pk)
        try:
            result = cls.model_validate(document)
        except TypeError as e:
            log.warning(
                f'Could not parse Redis response. Error was: "{e}". Probably, the '
                "connection is not set to decode responses from bytes. "
                "Attempting to decode response using the encoding set on "
                f"model class ({cls.__class__}. Encoding: {cls.Meta.encoding}."
            )
            document = decode_redis_value(document, cls.Meta.encoding)
            document = convert_empty_strings_to_none(document, model_fields)
            document = convert_base64_to_bytes(document, model_fields)
            document = restore_missing_pk(cls, document, pk)
            result = cls.model_validate(document)
        return result

    @classmethod
    async def get_many(
        cls: Type["Model"],
        pks: Sequence[Any],
        pipeline: Optional[redis.client.Pipeline] = None,
    ) -> Sequence["Model"]:
        """Retrieve multiple HashModel instances by primary key using a pipeline."""
        if not pks:
            return []
        keys = [cls.make_primary_key(pk) for pk in pks]
        if pipeline is not None:
            for key in keys:
                pipeline.hgetall(key)
            return []  # caller will execute the pipeline
        db = cls.db().pipeline(transaction=False)
        for key in keys:
            db.hgetall(key)
        results = await db.execute()
        model_fields = get_model_fields(cls)
        models = []
        for requested_pk, document in zip(pks, results):
            if not document:
                continue
            document = convert_empty_strings_to_none(document, model_fields)
            document = convert_base64_to_bytes(document, model_fields)
            document = restore_missing_pk(cls, document, requested_pk)
            try:
                models.append(cls.model_validate(document))
            except TypeError:
                document = decode_redis_value(document, cls.Meta.encoding)
                document = convert_empty_strings_to_none(document, model_fields)
                document = convert_base64_to_bytes(document, model_fields)
                document = restore_missing_pk(cls, document, requested_pk)
                models.append(cls.model_validate(document))
        return models

    @classmethod
    def redisearch_schema(cls):
        hash_prefix = cls.make_key(cls._meta.primary_key_pattern.format(pk=""))
        schema_prefix = f"ON HASH PREFIX 1 {hash_prefix} SCHEMA"
        schema_parts = [schema_prefix] + cls.schema_for_fields()
        return " ".join(schema_parts)

    async def update(self, **field_values):
        validate_model_fields(self.__class__, field_values)
        for field, value in field_values.items():
            setattr(self, field, value)
        await self.save()

    # ── per-field expiration (Redis 7.4+ HEXPIRE family) ─────────
    #
    # These methods wrap the per-field TTL commands introduced in Redis
    # Community Edition 7.4 (HEXPIRE, HPEXPIRE, HEXPIREAT, HPEXPIREAT,
    # HPERSIST, HEXPIRETIME, HPEXPIRETIME, HTTL, HPTTL). They operate on
    # the model's own Redis key, so callers just pass field names.
    #
    # The redis-py 8.0+ client exposes native high-level methods for all
    # of these (db.hexpire, db.httl, ...); we use them so argument
    # ordering and response parsing match the redis-py contract exactly.

    async def set_field_ttl(
        self, field: str, ttl_seconds: int, *, px: bool = False
    ) -> int:
        """Set a TTL on a single hash field (``HEXPIRE``/``HPEXPIRE``).

        Args:
            field: The hash field name.
            ttl_seconds: Time-to-live.  Seconds by default; milliseconds
                when ``px=True`` (uses ``HPEXPIRE``).

        Returns:
            ``1`` on success, ``-2`` if the field or key doesn't exist.
        """
        db = self.db()
        if px:
            results = await db.hpexpire(self.key(), int(ttl_seconds), field)
        else:
            results = await db.hexpire(self.key(), int(ttl_seconds), field)
        return int(results[0])

    async def set_field_ttl_at(
        self, field: str, unix_timestamp: int, *, px: bool = False
    ) -> int:
        """Set field expiration to a UNIX timestamp (``HEXPIREAT``/``HPEXPIREAT``).

        Args:
            field: The hash field name.
            unix_timestamp: Absolute expiry.  Seconds by default;
                milliseconds when ``px=True``.

        Returns:
            ``1`` on success, ``-2`` if the field or key doesn't exist.
        """
        db = self.db()
        if px:
            results = await db.hpexpireat(self.key(), int(unix_timestamp), field)
        else:
            results = await db.hexpireat(self.key(), int(unix_timestamp), field)
        return int(results[0])

    async def get_field_ttl(self, field: str, *, px: bool = False) -> int:
        """Get the remaining TTL of a field (``HTTL``/``HPTTL``).

        Returns:
            Remaining time (seconds or ms), ``-1`` if the field has no
            expiry, ``-2`` if the field or key doesn't exist.
        """
        db = self.db()
        if px:
            results = await db.hpttl(self.key(), field)
        else:
            results = await db.httl(self.key(), field)
        return int(results[0])

    async def get_field_expire_time(self, field: str, *, px: bool = False) -> int:
        """Get the absolute expiration timestamp of a field
        (``HEXPIRETIME``/``HPEXPIRETIME``).

        Returns:
            Absolute expiry (seconds or ms since epoch), ``-1`` if no
            expiry, ``-2`` if the field or key doesn't exist.
        """
        db = self.db()
        if px:
            results = await db.hpexpiretime(self.key(), field)
        else:
            results = await db.hexpiretime(self.key(), field)
        return int(results[0])

    async def persist_field(self, field: str) -> int:
        """Remove the TTL from a single field (``HPERSIST``).

        Returns:
            ``1`` on success, ``-2`` if the field or key doesn't exist.
        """
        db = self.db()
        results = await db.hpersist(self.key(), field)
        return int(results[0])

    async def expire_fields(
        self, ttl_seconds: int, *fields: str, px: bool = False
    ) -> List[int]:
        """Set the same TTL on multiple fields at once (``HEXPIRE``/``HPEXPIRE``).

        Returns a list of per-field results (``1``/``-2``).
        """
        db = self.db()
        if px:
            results = await db.hpexpire(self.key(), int(ttl_seconds), *fields)
        else:
            results = await db.hexpire(self.key(), int(ttl_seconds), *fields)
        return [int(r) for r in results]

    # ── HGETEX / HSETEX / HGETDEL (Redis 8.0+) ─────────────────────
    #
    # These three commands have no redis-py high-level binding, so they
    # go through ``execute_command``.

    async def get_and_set_field_expiry(
        self, field: str, ttl_seconds: int
    ) -> Optional[str]:
        """Get a field's value and set its expiry in one round trip
        (``HGETEX ... EX``).

        Returns:
            The field's current value, or ``None`` if it doesn't exist.
        """
        db = self.db()
        raw = await db.execute_command(
            "HGETEX", self.key(), "EX", int(ttl_seconds), "FIELDS", 1, field
        )
        if not raw:
            return None
        return raw[0]

    async def set_fields_with_expiry(
        self, ttl_seconds: int, **field_values: str
    ) -> int:
        """Set multiple fields with a shared expiry (``HSETEX ... EX``).

        Returns the number of new fields created.
        """
        if not field_values:
            return 0
        db = self.db()
        fields = list(field_values.items())
        flat: List[Any] = []
        for name, value in fields:
            flat.extend([name, value])
        return await db.execute_command(
            "HSETEX",
            self.key(),
            "EX",
            int(ttl_seconds),
            "FIELDS",
            len(fields),
            *flat,
        )

    async def get_and_delete_field(self, field: str) -> Optional[str]:
        """Get a field's value and delete it atomically (``HGETDEL``).

        Returns:
            The field's previous value, or ``None`` if it didn't exist.
        """
        db = self.db()
        raw = await db.execute_command("HGETDEL", self.key(), "FIELDS", 1, field)
        if not raw:
            return None
        return raw[0]

    @classmethod
    def schema_for_fields(cls):
        schema_parts = []

        for name, field in cls.model_fields.items():
            # ``schema_for_fields`` handles primary-key fields, indexed fields,
            # container fields with embedded models, and embedded RedisModel
            # fields. ``schema_for_type`` is reused for the inner type of each
            # case but cannot be inlined here without losing the primary-key
            # separator handling and the container-vs-model dispatch logic.
            _type = outer_type_or_annotation(field)
            is_subscripted_type = get_origin(_type)
            field_info = field

            if getattr(field_info, "primary_key", None):
                primary_key_type = _type
                if not isinstance(primary_key_type, type):
                    type_args = typing_get_args(primary_key_type)
                    primary_key_type = next(
                        (
                            arg
                            for arg in type_args
                            if isinstance(arg, type) and arg is not type(None)  # noqa: E721
                        ),
                        str,
                    )
                if isinstance(primary_key_type, type) and issubclass(
                    primary_key_type, str
                ):
                    separator = getattr(
                        field_info, "separator", SINGLE_VALUE_TAG_FIELD_SEPARATOR
                    )
                    redisearch_field = f"{name} TAG SEPARATOR {separator}"
                else:
                    redisearch_field = cls.schema_for_type(name, _type, field_info)
                schema_parts.append(redisearch_field)
            elif should_index_field(field_info):
                schema_parts.append(cls.schema_for_type(name, _type, field_info))
            elif is_subscripted_type:
                # Ignore subscripted types (usually containers!) that we don't
                # support, for the purposes of indexing.
                if not is_supported_container_type(_type):
                    continue

                embedded_cls = get_args(_type)
                if not embedded_cls:
                    # Bare ``List`` or ``Tuple`` without a type parameter
                    # (e.g. ``x: list`` instead of ``x: List[str]``). We can't
                    # infer a schema without knowing the inner type, so warn and
                    # skip rather than emit a malformed RediSearch schema.
                    log.warning("Model %s defined an empty list field: %s", cls, name)
                    continue
                embedded_cls = embedded_cls[0]
                schema_parts.append(cls.schema_for_type(name, embedded_cls, field_info))
            elif issubclass(_type, RedisModel):
                schema_parts.append(cls.schema_for_type(name, _type, field_info))
        return schema_parts

    @classmethod
    def schema_for_type(cls, name, typ: Any, field_info: PydanticFieldInfo):
        # Per-type schema-string construction (TAG, NUMERIC, GEO, etc.) lives
        # inline here. A future refactor could move each branch into its own
        # small builder class keyed on type, but the current explicit dispatch
        # is readable and keeps all schema logic in one place.
        typ = _unwrap_type_annotation(typ)
        sortable = getattr(field_info, "sortable", False)
        case_sensitive = getattr(field_info, "case_sensitive", False)

        if is_supported_container_type(typ):
            embedded_cls = get_args(typ)
            if not embedded_cls:
                # Bare ``List``/``Tuple`` without a type parameter. Without an
                # inner type we can't emit a meaningful RediSearch schema, so
                # warn and return an empty string to skip this field.
                log.warning(
                    "Model %s defined an empty list or tuple field: %s", cls, name
                )
                return ""
            embedded_cls = embedded_cls[0]
            if sortable is True:
                raise ValueError(
                    f"Field '{name}' is a container type and cannot be marked as sortable.\
                         Mark individual fields within the embedded model as sortable instead."
                )
            schema = cls.schema_for_type(name, embedded_cls, field_info)
        elif typ is bool:
            schema = f"{name} TAG"
        elif is_numeric_type(typ):
            vector_options: Optional[VectorFieldOptions] = getattr(
                field_info, "vector_options", None
            )
            if vector_options:
                schema = f"{name} {vector_options.schema}"
            else:
                schema = f"{name} NUMERIC"
        elif typ is Coordinates:
            schema = f"{name} GEO"
        elif typ in (datetime.date, datetime.datetime):
            schema = f"{name} NUMERIC"
        elif isinstance(typ, type) and issubclass(typ, str):
            separator = getattr(
                field_info, "separator", SINGLE_VALUE_TAG_FIELD_SEPARATOR
            )
            if getattr(field_info, "full_text_search", False) is True:
                schema = f"{name} TAG SEPARATOR {separator} {name} AS {name}_fts TEXT"
            else:
                schema = f"{name} TAG SEPARATOR {separator}"
        elif isinstance(typ, type) and issubclass(typ, RedisModel):
            if sortable is True:
                raise ValueError(
                    f"Field '{name}' is an embedded model and cannot be marked as sortable.\
                          Mark individual fields within the embedded model as sortable instead."
                )
            sub_fields = []
            for embedded_name, field in typ.model_fields.items():
                # Skip the inherited pk field on embedded models — it is
                # not a real indexed field.
                if embedded_name == "pk" and getattr(
                    getattr(typ, "_meta", None), "embedded", False
                ):
                    continue
                sub_fields.append(
                    cls.schema_for_type(
                        f"{name}_{embedded_name}",
                        outer_type_or_annotation(field),
                        field,
                    )
                )
            schema = " ".join(sub_fields)
        else:
            separator = getattr(
                field_info, "separator", SINGLE_VALUE_TAG_FIELD_SEPARATOR
            )
            schema = f"{name} TAG SEPARATOR {separator}"
        if schema and sortable is True:
            schema += " SORTABLE"
        if schema and case_sensitive is True:
            schema += " CASESENSITIVE"

        return schema


class JsonModel(RedisModel, abc.ABC):
    @staticmethod
    def _extract_field_info(
        field: Any,
    ) -> Union[FieldInfo, PydanticFieldInfo, ModelField]:
        """
        Extract FieldInfo from a field, handling various Pydantic versions and formats.

        This method consolidates the logic for extracting field info from:
        - Direct FieldInfo instances
        - Fields with field_info attribute
        - Fields with metadata containing FieldInfo
        """
        if hasattr(field, "field_info"):
            return field.field_info
        elif (
            not isinstance(field, FieldInfo)
            and hasattr(field, "metadata")
            and field.metadata
            and isinstance(field.metadata[0], FieldInfo)
        ):
            return field.metadata[0]
        return field

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def save(
        self: "Model", pipeline: Optional[redis.client.Pipeline] = None
    ) -> "Model":
        self.check()
        db = self._get_db(pipeline)

        # Get model data and convert datetime objects to timestamps
        document = self.model_dump()
        document = convert_datetime_to_timestamp(document)
        document = convert_bytes_to_base64(document)
        document = convert_dataclasses_to_dicts(document)

        # ClusterPipeline commands must not be awaited; doing so consumes the
        # response instead of queuing the command for batch execution.
        if _is_cluster_pipeline(db):
            db.json().set(self.key(), Path.root_path(), document)
        else:
            await db.json().set(self.key(), Path.root_path(), document)
        await self.finalize_save(pipeline=pipeline)
        return self

    @classmethod
    async def all_pks(cls, count: Optional[int] = None):
        """Yield the primary keys of every stored instance of this model.

        Uses a type-filtered ``SCAN`` over the model's key prefix. The ReJSON
        module has historically reported its keys' ``TYPE`` as ``ReJSON-RL``;
        Redis 8.x and some forks/variants may instead report them as
        ``"JSON"``. We scan with ``ReJSON-RL`` first and, only if that yields
        no keys, fall back to ``JSON``. A key can have only one ``TYPE``, so
        the two scans are mutually exclusive in practice; we still dedupe
        defensively in case a server ever reports both names.
        """
        key_prefix = cls.make_key(cls._meta.primary_key_pattern.format(pk=""))
        db = cls.db()

        # SCAN keys are returned as ``bytes`` when ``decode_responses=False``
        # and ``str`` otherwise. We decode bytes using the model's encoding
        # (default ``utf-8``) before stripping the key prefix.
        def _decode(key):
            if isinstance(key, str):
                return remove_prefix(key, key_prefix)
            return remove_prefix(key.decode(cls.Meta.encoding), key_prefix)

        async def _scan_type(type_name):
            scan_kwargs: Dict[str, Any] = {"_type": type_name}
            if count is not None:
                scan_kwargs["count"] = count
            async for key in db.scan_iter(f"{key_prefix}*", **scan_kwargs):
                yield _decode(key)

        async def _generator():
            seen: Set[str] = set()
            found_any = False
            # Try the historical name first; only fall back if it produced
            # nothing (avoids a second SCAN on the common path).
            for type_name in ("ReJSON-RL", "JSON"):
                if type_name != "ReJSON-RL" and found_any:
                    break
                async for pk in _scan_type(type_name):
                    found_any = True
                    if pk not in seen:
                        seen.add(pk)
                        yield pk

        return _generator()

    async def update(self, **field_values):
        validate_model_fields(self.__class__, field_values)
        for field, value in field_values.items():
            # Handle the simple update case first, e.g. city="Happy Valley"
            if "__" not in field:
                setattr(self, field, value)
                continue

            # Handle the nested update field name case, e.g. address__city="Happy Valley"
            obj = self
            parts = field.split("__")
            path_to_field = parts[:-1]
            target_field = parts[-1]

            # Get the final object in a nested update field name, e.g. for
            # the string address__city, we want to get self.address.city
            for sub_field in path_to_field:
                obj = getattr(obj, sub_field)

            # Set the target field (the last "part" of the nested update
            # field name) to the target value.
            setattr(obj, target_field, value)
        await self.save()

    @classmethod
    async def get(cls: Type["Model"], pk: Any) -> "Model":
        document_data = await cls.db().json().get(cls.make_key(pk))
        if document_data is None:
            raise NotFoundError
        # Convert timestamps back to datetime objects before validation
        model_fields = get_model_fields(cls)
        document_data = convert_timestamp_to_datetime(document_data, model_fields)
        document_data = convert_base64_to_bytes(document_data, model_fields)
        document_data = restore_missing_pk(cls, document_data, pk)
        return validate_model_data(cls, document_data)

    @classmethod
    async def get_many(
        cls: Type["Model"],
        pks: Sequence[Any],
        pipeline: Optional[redis.client.Pipeline] = None,
    ) -> Sequence["Model"]:
        """Retrieve multiple JsonModel instances by primary key using a pipeline."""
        if not pks:
            return []
        keys = [cls.make_key(pk) for pk in pks]
        if pipeline is not None:
            for key in keys:
                pipeline.json().get(key)
            return []  # caller will execute the pipeline
        db = cls.db().pipeline(transaction=False)
        for key in keys:
            db.json().get(key)
        results = await db.execute()
        model_fields = get_model_fields(cls)
        models = []
        for requested_pk, document_data in zip(pks, results):
            if document_data is None:
                continue
            document_data = convert_timestamp_to_datetime(document_data, model_fields)
            document_data = convert_base64_to_bytes(document_data, model_fields)
            document_data = restore_missing_pk(cls, document_data, requested_pk)
            models.append(validate_model_data(cls, document_data))
        return models

    @classmethod
    def _resolve_field_path(cls, field_path: str) -> Tuple[str, Any, bool]:
        """Translate a model field path into a JSONPath expression plus the
        metadata needed to deserialize the retrieved sub-value(s).

        The field path uses the same ``__`` nested-field syntax as
        ``update()`` (e.g. ``"address__city"`` or ``"orders__items__name"``).
        A raw enhanced JSONPath string (one starting with ``"$"``) is also
        accepted and passed through unchanged; in that case no type
        information is resolved.

        Returns a tuple of ``(json_path, value_type, crosses_list)`` where
        ``value_type`` is the annotation of an individual returned value (used
        for type conversion) and ``crosses_list`` indicates whether the path
        descends into one or more arrays (in which case multiple values may be
        returned).
        """
        # Allow callers to pass a raw enhanced JSONPath directly (anything
        # starting with "$"). Model field names never start with "$", so this
        # prefix check unambiguously separates raw paths from "__" paths. For a
        # raw path we can't infer the Python type, so values are returned as-is.
        # We treat the path as multi-valued only when it contains a "[*]"
        # wildcard; callers needing other shapes can use ``raw=True``.
        if field_path.startswith("$"):
            return field_path, None, "[*]" in field_path

        parts = field_path.split("__")
        current_model: Any = cls
        json_path = "$"
        crosses_list = False
        value_type: Any = None

        for index, part in enumerate(parts):
            model_fields = get_model_fields(current_model)
            if part not in model_fields:
                raise QuerySyntaxError(
                    f"The field path '{field_path}' contains a field that does "
                    f"not exist on {cls.__name__}. The field is: {part}"
                )
            field = model_fields[part]
            outer_type = get_outer_type(field)
            is_last = index == len(parts) - 1

            if is_supported_container_type(outer_type):
                inner_args = get_args(outer_type)
                inner_type = inner_args[0] if inner_args else None
                json_path += f".{part}[*]"
                crosses_list = True
                current_model = inner_type
                if is_last:
                    value_type = inner_type
            else:
                json_path += f".{part}"
                current_model = outer_type
                if is_last:
                    value_type = outer_type
                elif not has_model_field_mapping(outer_type):
                    raise QuerySyntaxError(
                        f"The field path '{field_path}' tries to descend into "
                        f"'{part}', which is not an embedded model on "
                        f"{cls.__name__}."
                    )

        return json_path, value_type, crosses_list

    @classmethod
    def _convert_sub_value(cls, value: Any, value_type: Any) -> Any:
        """Deserialize a single JSON sub-value into the appropriate Python
        type (datetime/date, bytes, nested models) based on ``value_type``."""
        if value_type is None or value is None:
            return value
        fields: Mapping[str, Any] = {_SUB_VALUE_KEY: _SubValueField(value_type)}
        wrapper = convert_timestamp_to_datetime({_SUB_VALUE_KEY: value}, fields)
        wrapper = convert_base64_to_bytes(wrapper, fields)
        return wrapper[_SUB_VALUE_KEY]

    @classmethod
    async def get_value(cls, pk: Any, field_path: str, *, raw: bool = False) -> Any:
        """Retrieve a sub-value of a stored JSON document using a JSONPath,
        without loading and deserializing the entire document.

        This implements the Redis JSON "retrieve a sub-value" pattern
        (``JSON.GET key <jsonpath>``), which is more efficient than fetching
        the whole document when you only need one field.

        Args:
            pk: The primary key of the document.
            field_path: A nested model field path using ``__`` as the
                separator (e.g. ``"address__city"``), or a raw JSONPath string
                starting with ``"$"``.
            raw: When ``True``, return the value(s) exactly as Redis returns
                them, skipping type conversion and single-value unwrapping.

        Returns:
            The resolved sub-value. For a path that does not descend into an
            array, a single value is returned (or ``None`` if the path matches
            nothing). For a path that descends into one or more arrays, a list
            of matching values is returned.

        Raises:
            NotFoundError: If no document exists for ``pk``.
            QuerySyntaxError: If ``field_path`` references a field that does
                not exist on the model.
        """
        json_path, value_type, crosses_list = cls._resolve_field_path(field_path)
        key = cls.make_key(pk)
        result = await cls.db().json().get(key, json_path)

        # ``JSON.GET`` returns nil (None) only when the key itself is missing.
        # A path that exists but matches nothing yields an empty list under
        # enhanced ($) JSONPath syntax.
        if result is None:
            raise NotFoundError

        if raw:
            return result

        if isinstance(result, list):
            converted = [cls._convert_sub_value(item, value_type) for item in result]
            if crosses_list:
                return converted
            # A non-array path matches at most one value under enhanced ($)
            # JSONPath syntax. An empty list means the field exists in the
            # schema but is null/absent in this document, so we surface that as
            # a single ``None`` rather than an empty list.
            if not converted:
                return None
            return converted[0]

        # Defensive fallback: enhanced ($) JSONPath always yields a list, but
        # if a client/path returns a scalar directly we convert and return it.
        return cls._convert_sub_value(result, value_type)

    @classmethod
    def redisearch_schema(cls):
        key_prefix = cls.make_key(cls._meta.primary_key_pattern.format(pk=""))
        schema_prefix = f"ON JSON PREFIX 1 {key_prefix} SCHEMA"
        schema_parts = [schema_prefix] + cls.schema_for_fields()
        return " ".join(schema_parts)

    @classmethod
    def schema_for_fields(cls):
        schema_parts = []
        json_path = "$"

        fields = dict(cls.model_fields)

        for name, field in fields.items():
            _type = get_outer_type(field)
            if _type is None:
                continue

            field_info = cls._extract_field_info(field)

            # Call schema_for_type for both primary_key and indexed fields
            # The method handles the distinction internally
            schema_parts.append(
                cls.schema_for_type(json_path, name, "", _type, field_info)
            )

        return schema_parts

    @classmethod
    def schema_for_type(
        cls,
        json_path: str,
        name: str,
        name_prefix: str,
        typ: Any,
        field_info: Union[PydanticFieldInfo, ModelField],
        parent_type: Optional[Any] = None,
    ) -> str:
        typ = _unwrap_type_annotation(typ)
        should_index = should_index_field(field_info)
        is_container_type = is_supported_container_type(typ)
        parent_is_container_type = is_supported_container_type(parent_type)

        try:
            field_is_model = issubclass(typ, RedisModel)
        except TypeError:
            # Not a class, probably a type annotation
            field_is_model = False

        vector_options: Optional[VectorFieldOptions] = getattr(
            field_info, "vector_options", None
        )
        try:
            is_vector = vector_options and has_numeric_inner_type(typ)
        except IndexError:
            raise RedisModelError(
                f"Vector field '{name}' must be annotated as a container type"
            )

        # When we encounter a list or model field, we need to descend
        # into the values of the list or the fields of the model to
        # find any values marked as indexed.
        if is_container_type and not is_vector:
            field_type = get_origin(typ)
            if field_type == Literal:
                path = f"{json_path}.{name}"
                return cls.schema_for_type(
                    path,
                    name,
                    name_prefix,
                    str,
                    field_info,
                    parent_type=field_type,
                )
            else:
                embedded_cls = get_args(typ)
                if not embedded_cls:
                    log.warning(
                        "Model %s defined an empty list or tuple field: %s", cls, name
                    )
                    return ""
                path = f"{json_path}.{name}[*]"
                embedded_cls = embedded_cls[0]
                return cls.schema_for_type(
                    path,
                    name,
                    name_prefix,
                    embedded_cls,
                    field_info,
                    parent_type=field_type,
                )
        elif field_is_model:
            name_prefix = f"{name_prefix}_{name}" if name_prefix else name
            sub_fields = []
            for embedded_name, field in typ.model_fields.items():
                # Skip the inherited pk field on embedded models — it is
                # not a real indexed field and should not appear in the
                # RediSearch schema.
                if embedded_name == "pk" and getattr(
                    getattr(typ, "_meta", None), "embedded", False
                ):
                    continue
                field_info = field
                if parent_is_container_type:
                    # We'll store this value either as a JavaScript array, so
                    # the correct JSONPath expression is to refer directly to
                    # attribute names after the container notation, e.g.
                    # orders[*].created_date.
                    path = json_path
                else:
                    # All other fields should use dot notation with both the
                    # current field name and "embedded" field name, e.g.,
                    # order.address.street_line_1.
                    path = f"{json_path}.{name}"
                sub_fields.append(
                    cls.schema_for_type(
                        path,
                        embedded_name,
                        name_prefix,
                        # field.annotation,
                        get_outer_type(field),
                        field_info,
                        parent_type=typ,
                    )
                )
            return " ".join(filter(None, sub_fields))
        # NOTE: This is the termination point for recursion. We've descended
        # into models and lists until we found an actual value to index.
        elif should_index:
            index_field_name = f"{name_prefix}_{name}" if name_prefix else name
            if parent_is_container_type:
                # If we're indexing the this field as a JavaScript array, then
                # the currently built-up JSONPath expression will be
                # "field_name[*]", which is what we want to use.
                path = json_path
            else:
                path = f"{json_path}.{name}"
            sortable = getattr(field_info, "sortable", False)
            case_sensitive = getattr(field_info, "case_sensitive", False)
            full_text_search = getattr(field_info, "full_text_search", False)
            sortable_tag_error = RedisModelError(  # noqa: F841
                "In this Preview release, TAG fields cannot "
                f"be marked as sortable. Problem field: {name}."
            )

            # For more complicated compound validators (e.g. PositiveInt), we might get a _GenericAlias rather than
            # a proper type, we can pull the type information from the origin of the first argument.
            if not isinstance(typ, type):
                # Handle Literal types: resolve to the type of the literal values
                if get_origin(typ) is Literal:
                    type_args = typing_get_args(typ)
                    typ = type(type_args[0]) if type_args else str
                else:
                    type_args = typing_get_args(typ)
                    typ = (
                        getattr(type_args[0], "__origin__", type_args[0])
                        if type_args
                        else typ
                    )

            if is_vector and vector_options:
                schema = f"{path} AS {index_field_name} {vector_options.schema}"
            elif parent_is_container_type:
                # only restrict the inner type to ``str`` when the field
                # is a bare list of scalars (e.g. ``List[str]``).
                # Fields inside an embedded model (e.g. ``List[Model]`` with
                # ``Model.zip_code: int``) inherit the model's type rules.
                if typ is not str:
                    raise RedisModelError(
                        "In this Preview release, list and tuple fields can only "
                        f"contain strings. Problem field: {name}."
                    )
                if sortable is True:
                    raise RedisModelError(
                        "In this Preview release, list and tuple fields cannot be "
                        f"marked as sortable. Problem field: {name}."
                    )
                if case_sensitive is True and full_text_search is True:
                    raise RedisModelError(
                        f"List field '{name}' cannot be both case-sensitive and "
                        "full-text searchable."
                    )
                separator = getattr(
                    field_info, "separator", SINGLE_VALUE_TAG_FIELD_SEPARATOR
                )
                schema = f"{path} AS {index_field_name} TAG SEPARATOR {separator}"
                if full_text_search is True:
                    schema += f" {path} AS {index_field_name}_fts TEXT"
                elif case_sensitive is True:
                    schema += " CASESENSITIVE"
            elif typ is bool:
                schema = f"{path} AS {index_field_name} TAG"
                if sortable is True:
                    schema += " SORTABLE"
            elif typ is Coordinates:
                schema = f"{path} AS {index_field_name} GEO"
                if sortable is True:
                    schema += " SORTABLE"
            elif is_numeric_type(typ):
                schema = f"{path} AS {index_field_name} NUMERIC"
                if sortable is True:
                    schema += " SORTABLE"
            elif issubclass(typ, str):
                separator = getattr(
                    field_info, "separator", SINGLE_VALUE_TAG_FIELD_SEPARATOR
                )
                if full_text_search is True:
                    schema = (
                        f"{path} AS {index_field_name} TAG SEPARATOR {separator} "
                        f"{path} AS {index_field_name}_fts TEXT"
                    )
                    if sortable is True:
                        # NOTE: With the current preview release, making a field
                        # full-text searchable and sortable only makes the TEXT
                        # field sortable. This means that results for full-text
                        # search queries can be sorted, but not exact match
                        # queries.
                        schema += " SORTABLE"
                    if case_sensitive is True:
                        raise RedisModelError("Text fields cannot be case-sensitive.")
                else:
                    # String fields are indexed as TAG fields and can be sortable
                    schema = f"{path} AS {index_field_name} TAG SEPARATOR {separator}"
                    if sortable is True:
                        schema += " SORTABLE"
                    if case_sensitive is True:
                        schema += " CASESENSITIVE"
            else:
                # Default to TAG field, which can be sortable
                separator = getattr(
                    field_info, "separator", SINGLE_VALUE_TAG_FIELD_SEPARATOR
                )
                schema = f"{path} AS {index_field_name} TAG SEPARATOR {separator}"
                if sortable is True:
                    schema += " SORTABLE"

            return schema
        return ""


class EmbeddedJsonModel(JsonModel, abc.ABC):
    class Meta:
        embedded = True

    def _model_dump_exclude(self, exclude: Any) -> Any:
        """EmbeddedJsonModel always excludes pk, regardless of its value."""
        if exclude is None:
            exclude = {"pk"}
        elif isinstance(exclude, AbstractSet):
            exclude = set(exclude)
            exclude.add("pk")
        elif isinstance(exclude, dict):
            exclude = dict(exclude)
            exclude["pk"] = True
        return exclude

    @model_validator(mode="before")
    @classmethod
    def _strip_stale_pk(cls, data: Any) -> Any:
        """Strip any stale or invalid pk from embedded model input.

        Old Redis records (or hand-crafted test data) may carry a ``pk`` key
        with an invalid type (e.g. ``[]``) or a stale string value.  Embedded
        models never have a meaningful pk, so we drop the key unconditionally
        before Pydantic validates the fields.
        """
        if isinstance(data, dict):
            data.pop("pk", None)
        return data
