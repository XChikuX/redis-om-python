"""Protocol-aware parsers for RediSearch (FT.*) responses.

redis-py 8.x auto-negotiates RESP3 against Redis 6+ servers (including the
Redis 8.x line referenced in ``pyproject.toml``).  This produces
fundamentally different wire shapes for several RediSearch commands compared
to RESP2.  redis-py's high-level ``Search.search()`` API installs protocol
callbacks that convert RESP3 dicts into the legacy ``Result``/``Document``
shapes, but pyredis-om calls ``execute_command`` directly because it needs
raw responses for pipelined bulk operations (e.g. ``get_many``).  As a
result, we still receive the raw RESP3 dicts on the wire and need our own
shim to normalise both wire formats into a single legacy shape.

This module centralises parsing for the FT.SEARCH / FT.AGGREGATE /
FT.AGGREGATE WITHCURSOR responses used by pyredis-om, so that the rest of the
codebase can operate on a single normalised form regardless of the wire
protocol the user negotiated.

The two wire formats we need to handle are:

* **RESP2 FT.SEARCH / FT.AGGREGATE**::

      [count, k1, [f1, v1, f2, v2, ...], k2, [...], ...]

  ``FT.AGGREGATE`` rows look the same but the first row contains the count
  of groups, not documents.

* **RESP3 FT.SEARCH / FT.AGGREGATE**::

      {
        "total_results": <int>,
        "results": [
          {
            "id": "<key>",
            "extra_attributes": {<field>: <value>, ...},
            "values": [<score_field>, <score_value>, ...],
          },
          ...
        ],
        "format": "STRING",
        "attributes": [...],
        "warning": [...],
      }

* **RESP2 FT.AGGREGATE WITHCURSOR**::

      [aggregate_result_array, cursor_id]

  ``FT.CURSOR READ`` returns the same flat shape as ``FT.AGGREGATE``.

* **RESP3 FT.AGGREGATE WITHCURSOR**::

      [
        <aggregate_dict>,
        <cursor_id>,
      ]

  ``FT.CURSOR READ`` returns the dict form.

The functions in this module accept either raw response and return either a
normalised tuple of ``(total, rows)`` (for FT.SEARCH / FT.AGGREGATE) or
``(rows, cursor_id)`` (for FT.AGGREGATE WITHCURSOR).
"""

from typing import Any, Dict, List, Optional, Sequence, Tuple

# Type aliases used by callers. ``DocumentRow`` mimics the legacy
# ``[key, fields_list]`` shape so the rest of the codebase only has to know
# about one representation.
DocumentRow = List[Any]


def _decode_text(value: Any) -> Any:
    """Best-effort decode of a single string-like value."""
    if isinstance(value, bytes):
        return value.decode("utf-8", "ignore")
    return value


def _normalise_field_value(value: Any) -> List[Any]:
    """Convert a single RediSearch field value to ``[name, value]`` form."""
    return [_decode_text(value), value]


# Known RESP3 FT.SEARCH / FT.AGGREGATE top-level keys, in both ``str`` and
# ``bytes`` forms. redis-py surfaces RESP3 map keys as bytes for raw
# ``execute_command`` calls even when ``decode_responses=True`` is set, so we
# have to match both variants everywhere we inspect the dict.
_RESP3_SEARCH_KEYS_STR = ("results", "total_results")
_RESP3_SEARCH_KEYS_BYTES = tuple(k.encode("utf-8") for k in _RESP3_SEARCH_KEYS_STR)


def is_resp3_search_response(raw: Any) -> bool:
    """Return True if ``raw`` matches the RESP3 FT.SEARCH / FT.AGGREGATE shape.

    Accepts dicts whose keys are either ``str`` or ``bytes`` because redis-py
    does not always decode RESP3 map keys for raw ``execute_command`` callers.
    """
    if not isinstance(raw, dict):
        return False

    for str_key in _RESP3_SEARCH_KEYS_STR:
        if str_key in raw:
            return True
    for bytes_key in _RESP3_SEARCH_KEYS_BYTES:
        if bytes_key in raw:
            return True
    return False


def _decode_dict_keys(d: Any) -> Dict[str, Any]:
    """Return a copy of ``d`` with bytes keys decoded to ``str``.

    Used to normalise RESP3 dict responses whose keys arrive as ``bytes``.
    Non-bytes keys are preserved unchanged. Values are left untouched; the
    rest of the shim and ``from_redis`` already handle bytes values where it
    matters (e.g. via :func:`_decode_text`).
    """
    if not isinstance(d, dict):
        return d
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(k, bytes):
            out[k.decode("utf-8", "ignore")] = v
        else:
            out[k] = v
    return out


def _resp2_row_to_key_fields(row: Any) -> Optional[List[Any]]:
    """Convert a RESP2 row into the flat-pair normalised form.

    ``row`` is expected to be a sequence of ``[name, value, name, value, ...]``
    as produced by RediSearch on RESP2.  Returns ``None`` when the row is
    empty or already ``None`` (NOCONTENT results).
    """
    if not row:
        return None
    return list(row)


def _resp3_row_to_key_fields(row: Any) -> Optional[List[Any]]:
    """Convert a RESP3 RediSearch row dict into the legacy flat-pair form.

    Returns a single ``[name, value, name, value, ...]`` list that the rest
    of the pyredis-om code can iterate over with ``range(0, len(row), 2)``.
    The document key (when present in FT.SEARCH responses) is included as
    the first pair so ``_pk_from_redis_key`` keeps working.
    """
    if not isinstance(row, dict):
        return None
    flat: List[Any] = []
    key = row.get("id")
    if key is not None:
        flat.append("id")
        flat.append(key)
    extra = row.get("extra_attributes") or {}
    if isinstance(extra, dict):
        for name, value in extra.items():
            flat.append(_decode_text(name))
            flat.append(value)
    values = row.get("values") or []
    if isinstance(values, list):
        # ``values`` is a list of ``[score_name, score_value]`` pairs.
        for item in values:
            if isinstance(item, list) and len(item) == 2:
                flat.append(_decode_text(item[0]))
                flat.append(item[1])
    return flat


def split_search_response(
    raw: Any,
    protocol: Optional[int] = None,
    command: str = "search",
) -> Tuple[int, List[List[Any]]]:
    """Normalise a raw FT.SEARCH / FT.AGGREGATE response.

    Returns a ``(total, rows)`` tuple where ``total`` is the integer result
    count and ``rows`` is a list of flat ``[name, value, name, value, ...]``
    lists.  Each row's fields can be iterated with ``range(0, len(row), 2)``
    by callers that don't care about the protocol.

    The ``command`` argument (``"search"`` or ``"aggregate"``) only affects
    the RESP2 layout because the two commands use different wire shapes:

    * RESP2 ``FT.SEARCH`` returns ``[count, key1, [fields1], key2, ...]``.
    * RESP2 ``FT.AGGREGATE`` returns ``[count, row1, row2, ...]`` where each
      row is already a flat-pair list.

    When ``protocol`` is omitted the function sniffs the wire shape so it
    works on either protocol transparently.
    """
    if protocol == 3 or (protocol is None and is_resp3_search_response(raw)):
        # redis-py may surface RESP3 map keys as bytes for raw
        # ``execute_command`` callers, so normalise them up front.
        raw = _decode_dict_keys(raw)
        total = int(raw.get("total_results", 0) or 0)
        rows: List[List[Any]] = []
        for entry in raw.get("results") or []:
            flat = _resp3_row_to_key_fields(_decode_dict_keys(entry))
            if flat is None:
                continue
            rows.append(flat)
        return total, rows

    if isinstance(raw, (list, tuple)) and raw:
        total = int(raw[0] or 0)
        rows = []
        if command == "search":
            # RESP2 FT.SEARCH layout: [count, key, [fields], key, [fields], ...]
            i = 1
            while i + 1 < len(raw):
                key = raw[i]
                fields = raw[i + 1]
                if fields is None:
                    i += 2
                    continue
                flat = []
                if key is not None:
                    flat.append("__key")
                    flat.append(key)
                if isinstance(fields, (list, tuple)):
                    flat.extend(fields)
                rows.append(flat)
                i += 2
        else:
            # RESP2 FT.AGGREGATE layout: [count, row1, row2, ...] where each
            # row is a flat-pair list (or nested dict for RESP3-style responses
            # which we've already handled above).
            for entry in raw[1:]:
                if isinstance(entry, (list, tuple)):
                    rows.append(list(entry))
                elif isinstance(entry, dict):
                    flat = _resp3_row_to_key_fields(entry)
                    if flat is not None:
                        rows.append(flat)
        return total, rows

    return 0, []


def split_cursor_response(raw: Any, protocol: Optional[int] = None) -> Tuple[Any, int]:
    """Normalise an FT.AGGREGATE WITHCURSOR response.

    Returns ``(aggregate_result, cursor_id)`` where ``aggregate_result`` is in
    the same normalised form as :func:`split_search_response` (a list of
    flat-pair rows) and ``cursor_id`` is an integer.  This works for both the
    initial WITHCURSOR response and subsequent FT.CURSOR READ responses.
    """
    if not raw:
        return [], 0

    # RESP3 produces ``[aggregate_dict, cursor_id]`` for both WITHCURSOR and
    # FT.CURSOR READ.  FT.CURSOR READ with no remaining rows returns just the
    # dict and the cursor_id stays 0.
    if (
        isinstance(raw, list)
        and len(raw) == 2
        and isinstance(raw[1], (int, str, bytes))
    ):
        cursor_id = int(raw[1])
        inner = raw[0]
    elif isinstance(raw, dict):
        cursor_id = 0
        inner = raw
    else:
        # Fallback: legacy response with cursor at index 1.
        cursor_id = 0
        inner = raw

    if isinstance(inner, dict):
        # RESP3 shape: dict with ``results``.
        if protocol == 3 or (protocol is None and is_resp3_search_response(inner)):
            _, rows = split_search_response(inner, protocol=3, command="aggregate")
            return rows, cursor_id

    # RESP2 FT.AGGREGATE WITHCURSOR shape: ``[count, row1, row2, ...]``.
    if isinstance(inner, (list, tuple)):
        _, rows = split_search_response(inner, protocol=2, command="aggregate")
        return rows, cursor_id

    return [], cursor_id


def extract_key_from_row(row: Sequence[Any]) -> Optional[str]:
    """Return the ``__key`` value from a normalised FT.AGGREGATE row.

    The row is expected to be the legacy ``[name, value, name, value, ...]``
    list.  Bytes keys are decoded to ``str`` to match the historical return
    type.
    """
    if not row:
        return None
    for i in range(0, len(row), 2):
        name = _decode_text(row[i])
        if name == "__key":
            return _decode_text(row[i + 1])
    return None
