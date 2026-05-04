# Pydantic v2 Upgrade Notes

This repository now targets **Pydantic v2** as its primary and only supported Pydantic runtime.

## What changed

- Dropped the `pydantic.v1` compatibility path.
- Switched Redis OM internals to native Pydantic v2 validation and serialization APIs:
  - `model_validate()`
  - `model_dump()`
  - `model_config = ConfigDict(...)`
  - `@field_validator`
  - `@model_validator`
- Updated the custom model metaclass so Redis OM models can be defined with native v2 validators, including `EmbeddedJsonModel` classes.
- Kept a small `dict()` wrapper on Redis OM models for compatibility, but the internal implementation is now `model_dump()`-based.

## Issues that had to be finalized for the upgrade

### 1. Model class creation

The old implementation built models through Pydantic v1 internals and converted v2 validators back into v1 decorators. That prevented native `@model_validator` usage from working reliably on Redis OM models.

### 2. Validation/deserialization

Redis payload hydration now uses `model_validate()` directly instead of `parse_obj()` fallbacks.

### 3. Serialization

Redis save paths now use `model_dump()` so nested models and embedded models follow Pydantic v2 semantics.

### 4. Embedded-model PK handling

Embedded models still omit null `pk` values from dumped payloads, including when nested inside other Redis OM models, while preserving explicitly assigned `pk` values.

### 5. Field metadata

Redis OM-specific field options (`primary_key`, `index`, `sortable`, `full_text_search`, `separator`, `vector_options`, etc.) are preserved on top of Pydantic v2 field objects and continue to drive schema generation and query behavior.

### 6. User-facing configuration

Examples and docs should use:

- `model_config = ConfigDict(...)`
- `@field_validator`
- `@model_validator`
- `model_dump()`
- `model_validate()`

and should no longer use:

- `class Config`
- `@validator`
- `@root_validator`
- `parse_obj()`

## Notes from reviewing upstream

`redis/redis-om-python` already moved toward first-class Pydantic v2 support, which confirmed the direction of this upgrade. This fork intentionally keeps its own Redis OM behavior and fixes, and only borrows ideas where they fit the existing architecture cleanly.
