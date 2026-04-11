# mypy: ignore-errors

try:
    from pydantic.v1 import EmailStr, PositiveInt, ValidationError
except ImportError:
    from pydantic import EmailStr, PositiveInt, ValidationError
