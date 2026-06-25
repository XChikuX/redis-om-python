"""Vector Sets — high-dimensional similarity search keyed by element name.

Redis 8.8 ships a preview module (``@vectorset``) that adds a new HNSW-backed
data type for approximate nearest-neighbour (ANN) search over floating-point
vectors. Unlike ``FT.HNSW`` / RediSearch vector fields, vector sets are a
**first-class data structure**: each element has a name and may carry its own
JSON attributes, and ``VSIM`` accepts raw query vectors without needing a
separate index.

Supported commands (all probed on Redis 8.8.0):

* ``VADD``           — add a vector element (with optional ``REDUCE``,
                       ``CAS``, quantization, ``EF``).
* ``VSIM``           — k-NN query against a vector set, supporting
                       ``COUNT``, ``EF``, ``FILTER`` (JSONPath expression),
                       ``WITHSCORES``, ``WITHATTRIBS``, and ``EPSILON``.
* ``VCARD``          — number of elements in a set.
* ``VINFO``          — set metadata (quant type, dim, HNSW params, ...).
* ``VDIM``           — stored vector dimensionality.
* ``VEMB``           — retrieve the (quantized) stored vector for an element.
* ``VLINKS``         — HNSW neighbours of an element.
* ``VRANDMEMBER``    — random element(s).
* ``VREM``           — remove an element.
* ``VSETATTR``/``VGETATTR`` — get/set JSON attributes for an element.

All methods use ``execute_command`` so they work on any redis-py client that
speaks RESP2/RESP3. On Redis < 8.8 the server will raise ``ResponseError`` —
use :func:`has_vector_sets` to guard caller code.

Example::

    import asyncio
    from aredis_om import VectorSet

    async def main():
        vs = VectorSet(db, "vset:docs")
        await vs.add([1.0, 2.0, 3.0], "doc1")
        await vs.add([4.0, 5.0, 6.0], "doc2")
        results = await vs.similar([1.0, 2.0, 3.0], count=10, with_scores=True)
        print(results)  # [("doc1", 1.0), ("doc2", 0.9879...)]
"""

from __future__ import annotations

import json
from typing import Any, Mapping, Optional, Sequence, Union

# Cache: connection id → set of unsupported command names.
_capability_cache: "dict[int, set]" = {}

# ── helpers ─────────────────────────────────────────────────────────────

QuantType = str  # one of: "NOQUANT", "Q8", "BIN"
Score = float


def _fmt_vector(values: Sequence[float]) -> list:
    """Render a python float vector as the ``VALUES n v1 v2 ...`` token list."""
    return ["VALUES", len(values), *[float(v) for v in values]]


async def _probe(db: Any, command: str) -> bool:
    """Return True if ``command`` exists on this server."""
    cache = _capability_cache.setdefault(id(db), set())
    if command in cache:
        return False
    try:
        await db.execute_command("COMMAND", "INFO", command)
        return True
    except Exception:
        cache.add(command)
        return False


def _command_info_present(info: Any, name: str) -> bool:
    """Return True if a COMMAND INFO reply contains ``name``.

    Handles both RESP2 ``[[name, ...]]`` and RESP3 / redis-py 8
    ``{name: {...}}`` shapes.
    """
    if not info:
        return False
    name = name.lower()
    if isinstance(info, dict):
        return name in {k.lower() for k in info.keys()}
    # RESP2 list: ``[entry_or_none, ...]``.
    for entry in info:
        if entry is None:
            continue
        if isinstance(entry, (list, tuple)) and entry:
            if str(entry[0]).lower() == name:
                return True
    return False


# ── capability helpers ──────────────────────────────────────────────────


async def has_vector_sets(db: Any) -> bool:
    """Return ``True`` if the server has the ``VADD`` command (Redis 8.8+)."""
    try:
        info = await db.execute_command("COMMAND", "INFO", "VADD")
        return bool(_command_info_present(info, "vadd"))
    except Exception:
        return False


def clear_vector_set_cache() -> None:
    """Clear cached capability results (for tests)."""
    _capability_cache.clear()


# ── VectorSet wrapper ───────────────────────────────────────────────────


class VectorSet:
    """Thin wrapper around a Redis 8.8 vector-set key.

    All methods map 1-to-1 to underlying ``V*`` commands. Vector queries are
    specified as Python float lists and rendered into the server's ``VALUES``
    wire format. Set attributes are passed as dicts and serialised to JSON.

    Every command is sent via ``execute_command`` so the wrapper is
    agnostic to redis-py versions. Responses are normalised to consistent
    Python shapes regardless of whether the server replies in RESP2 or
    RESP3 form (``VSIM WITHSCORES`` returns a flat list under RESP2 but a
    map under RESP3).
    """

    def __init__(self, db: Any, key: str):
        self._db = db
        self._key = key

    @property
    def key(self) -> str:
        return self._key

    # ── mutation ────────────────────────────────────────────────────────

    async def add(
        self,
        vector: Sequence[float],
        element: str,
        *,
        reduce_to_dim: Optional[int] = None,
        cas: bool = False,
        quant: Optional[QuantType] = None,
        ef: Optional[int] = None,
    ) -> bool:
        """``VADD`` — add (or, with ``cas=True``, replace) a vector element.

        Args:
            vector: Input float vector. If ``reduce_to_dim`` is set this may
                be longer than the stored dimensionality.
            element: Name to associate with the vector.
            reduce_to_dim: If set, store a random projection down to this
                many dimensions (``REDUCE``).
            cas: Compare-and-swap — if ``element`` already exists, replace
                its stored vector with ``vector`` (otherwise no-op).
            quant: Quantization for new sets: ``"Q8"`` (8-bit, default),
                ``"BIN"`` (1-bit), or ``"NOQUANT"`` (full FP32). Ignored
                once the set exists.
            ef: Exploration factor at build time.

        Returns:
            ``True`` if a new element was added, ``False`` if ``cas=True``
            updated an existing element or the element already existed.
        """
        if quant not in (None, "Q8", "BIN", "NOQUANT"):
            raise ValueError(
                f"quant must be one of 'Q8', 'BIN', 'NOQUANT'; got {quant!r}"
            )
        args: list = ["VADD", self._key]
        if reduce_to_dim is not None:
            args += ["REDUCE", int(reduce_to_dim)]
        args += _fmt_vector(vector)
        args.append(element)
        if cas:
            args.append("CAS")
        if quant is not None:
            args.append(quant)
        if ef is not None:
            args += ["EF", int(ef)]
        return bool(await self._db.execute_command(*args))

    async def remove(self, element: str) -> bool:
        """``VREM`` — remove an element. Returns True if it existed."""
        return bool(await self._db.execute_command("VREM", self._key, element))

    # ── attributes ──────────────────────────────────────────────────────

    async def set_attribute(self, element: str, attributes: Mapping[str, Any]) -> bool:
        """``VSETATTR`` — attach JSON attributes to an element.

        ``attributes`` is serialised with :func:`json.dumps` and stored as
        the element's attribute blob. Pass an empty dict to clear.

        Returns ``True`` if the element existed (and the attribute was set).
        """
        payload = json.dumps(attributes)
        return bool(
            await self._db.execute_command("VSETATTR", self._key, element, payload)
        )

    async def get_attribute(self, element: str) -> Optional[Mapping[str, Any]]:
        """``VGETATTR`` — retrieve the attributes stored on an element.

        Returns the parsed JSON dict, or ``None`` if the element does not
        exist or has no attributes.
        """
        raw = await self._db.execute_command("VGETATTR", self._key, element)
        return _coerce_attrs(raw)

    # ── queries ─────────────────────────────────────────────────────────

    async def similar(
        self,
        vector: Sequence[float],
        *,
        count: Optional[int] = None,
        ef: Optional[int] = None,
        filter_expr: Optional[str] = None,
        epsilon: Optional[float] = None,
        with_scores: bool = False,
        with_attributes: bool = False,
    ) -> Union[list[str], list[tuple]]:
        """``VSIM`` — k-NN similarity query against a query vector.

        Args:
            vector: Query vector as a Python list of floats.
            count: Cap on the number of results (``COUNT``). Default: server
                default (10).
            ef: Exploration factor at query time. Higher = more accurate.
            filter_expr: JSONPath-style boolean expression evaluated against
                candidate attributes, e.g. ``'.color == "red"'``.
            epsilon: Search epsilon (``EPSILON``) — relaxes graph traversal.
            with_scores: Include similarity scores (``WITHSCORES``).
            with_attributes: Include per-result attributes
                (``WITHATTRIBS``).

        Returns:
            * If neither ``with_scores`` nor ``with_attributes``: list of
              element names.
            * If only ``with_scores``: list of ``(name, score)`` tuples.
            * If only ``with_attributes``: list of ``(name, attrs)`` tuples.
            * If both: list of ``(name, score, attrs)`` tuples.
        """
        args: list = ["VSIM", self._key]
        args += _fmt_vector(vector)
        if count is not None:
            args += ["COUNT", int(count)]
        if ef is not None:
            args += ["EF", int(ef)]
        if filter_expr is not None:
            args += ["FILTER", filter_expr]
        if epsilon is not None:
            args += ["EPSILON", float(epsilon)]
        if with_scores:
            args.append("WITHSCORES")
        if with_attributes:
            args.append("WITHATTRIBS")

        raw = await self._db.execute_command(*args)
        return _parse_vsim(raw, with_scores, with_attributes)

    # ── introspection ───────────────────────────────────────────────────

    async def card(self) -> int:
        """``VCARD`` — number of elements."""
        return int(await self._db.execute_command("VCARD", self._key))

    async def dim(self) -> int:
        """``VDIM`` — stored vector dimensionality."""
        return int(await self._db.execute_command("VDIM", self._key))

    async def info(self) -> dict:
        """``VINFO`` — set metadata as a dict."""
        raw = await self._db.execute_command("VINFO", self._key)
        if isinstance(raw, dict):
            return {str(k): v for k, v in raw.items()}
        return _pairs_to_dict(raw)

    async def embedding(self, element: str) -> list[float]:
        """``VEMB`` — stored (quantized) vector for an element."""
        raw = await self._db.execute_command("VEMB", self._key, element)
        return [float(x) for x in raw]

    async def links(self, element: str) -> list[list[str]]:
        """``VLINKS`` — HNSW neighbour lists per level for an element.

        Returns one list of neighbour names per level. (Level 0 is the
        finest, deepest layer of the HNSW graph.)
        """
        raw = await self._db.execute_command("VLINKS", self._key, element)
        out: list[list[str]] = []
        for layer in raw or []:
            if layer is None:
                out.append([])
            elif isinstance(layer, (list, tuple)):
                out.append([str(x) for x in layer])
            else:
                out.append([str(layer)])
        return out

    async def random_member(
            self, count: Optional[int] = None
        ) -> Union[None, str, list[str]]:
            """``VRANDMEMBER`` — fetch random element name(s).

            When ``count`` is ``None`` (default), returns a bare string (or
            ``None`` if the set is empty). When ``count`` is provided, returns
            a list of strings. Positive ``count`` returns distinct elements;
            negative allows repetition.
            """
        if count is None:
            raw = await self._db.execute_command("VRANDMEMBER", self._key)
            return raw if raw is None else str(raw)
        raw = await self._db.execute_command("VRANDMEMBER", self._key, int(count))
        return [str(x) for x in (raw or [])]


# ── response parsing ────────────────────────────────────────────────────


def _parse_vsim(
    raw: Any, with_scores: bool, with_attributes: bool
) -> Union[list[str], list[tuple]]:
    """Normalise a ``VSIM`` reply into Pythonic shapes.

    Handles three server reply shapes:
    * **RESP2** flat list: ``[name1, name2, ...]`` (no flags),
      ``[name1, score1, ...]`` (WITHSCORES), etc.
    * **RESP3 / redis-py 8** map: ``{name: score}`` (WITHSCORES),
      ``{name: attrs}`` (WITHATTRIBS), ``{name: [score, attrs]}`` (both).
    * **Bytes**: values may be bytes when ``decode_responses=False``.
    """
    if raw is None:
        return []

    # ── dict shape (RESP3 or redis-py 8 coercion) ──────────────────────
    if isinstance(raw, dict):
        if not with_scores and not with_attributes:
            return [str(k) for k in raw.keys()]
        if with_scores and not with_attributes:
            return [(_str(k), _num(v)) for k, v in raw.items()]
        if with_attributes and not with_scores:
            return [(_str(k), _coerce_attrs(v)) for k, v in raw.items()]
        # Both: values are [score, attrs].
        return [
            (_str(k), _num(v[0]), _coerce_attrs(v[1] if len(v) > 1 else None))
            for k, v in raw.items()
        ]

    # ── list shape (RESP2 flat) ────────────────────────────────────────
    items = list(raw)
    if not with_scores and not with_attributes:
        return [_str(x) for x in items]
    if with_scores and not with_attributes:
        return [(_str(items[i]), _num(items[i + 1])) for i in range(0, len(items), 2)]
    if with_attributes and not with_scores:
        return [
            (_str(items[i]), _coerce_attrs(items[i + 1]))
            for i in range(0, len(items), 2)
        ]
    # Both: triplets.
    return [
        (
            _str(items[i]),
            _num(items[i + 1]),
            _coerce_attrs(items[i + 2]),
        )
        for i in range(0, len(items), 3)
    ]


def _str(x: Any) -> str:
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8")
    return str(x)


def _num(x: Any) -> float:
    if isinstance(x, (bytes, bytearray)):
        return float(x.decode("utf-8"))
    return float(x)


def _coerce_attrs(raw: Any) -> Any:
    """Parse VSIM/VGETATTR payload, which may be ``nil`` or JSON."""
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return raw
    if isinstance(raw, dict):
        return raw
    return raw


def _pairs_to_dict(raw: Any) -> dict:
    """Flatten a RESP2 ``[k1, v1, k2, v2, ...]`` reply into a dict."""
    if raw is None:
        return {}
    out: dict = {}
    it = iter(raw)
    for k, v in zip(it, it):
        out[_str(k)] = v
    return out
