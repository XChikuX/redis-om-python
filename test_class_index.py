"""Empirical verification of class-level index=True support."""

import warnings
from typing import Optional, List

from aredis_om import EmbeddedJsonModel, JsonModel, Field, HashModel


def banner(name):
    print("\n" + "=" * 72)
    print(f"  {name}")
    print("=" * 72)


# ── Test 1: class JsonModel(index=True) auto-indexes all scalar fields ──────
banner("TEST 1: class JsonModel(index=True) — all fields auto-indexed")


class Customer(JsonModel, index=True):
    first_name: str
    last_name: str
    email: str
    age: int


print(f"  _meta.index_enabled = {Customer._meta.index_enabled}")
schema = Customer.redisearch_schema()
print(f"  schema = {schema}")
assert "$.first_name AS first_name TAG" in schema, "first_name should be indexed"
assert "$.last_name AS last_name TAG" in schema, "last_name should be indexed"
assert "$.email AS email TAG" in schema, "email should be indexed"
assert "$.age AS age NUMERIC" in schema, "age should be NUMERIC"
print("  [PASS] All fields auto-indexed")


# ── Test 2: Field(index=False) opts out of class-level indexing ─────────────
banner("TEST 2: Field(index=False) opts out")


class Customer2(JsonModel, index=True):
    first_name: str
    last_name: str = Field(index=False)  # explicit opt-out
    age: int


schema = Customer2.redisearch_schema()
print(f"  schema = {schema}")
assert "$.first_name AS first_name TAG" in schema, "first_name should be indexed"
assert "$.last_name" not in schema, "last_name should NOT be indexed"
assert "$.age AS age NUMERIC" in schema, "age should be indexed"
print("  [PASS] index=False honored")


# ── Test 3: No index=True anywhere — only pk indexed ────────────────────────
banner("TEST 3: Plain JsonModel (no index) — only pk")


class Customer3(JsonModel):
    first_name: str
    last_name: str
    age: int


schema = Customer3.redisearch_schema()
print(f"  schema = {schema}")
assert "$.first_name" not in schema, "first_name should NOT be indexed"
assert "$.last_name" not in schema, "last_name should NOT be indexed"
assert "$.age" not in schema, "age should NOT be indexed"
print("  [PASS] No auto-indexing without class-level flag")


# ── Test 4: EmbeddedJsonModel(index=True) auto-indexes sub-fields ───────────
banner("TEST 4: class EmbeddedJsonModel(index=True)")


class Address4(EmbeddedJsonModel, index=True):
    address_line_1: str
    address_line_2: Optional[str] = None
    city: str
    state: str
    postal_code: str


class Customer4(JsonModel, index=True):
    name: str
    address: Address4


schema = Customer4.redisearch_schema()
print(f"  schema = {schema}")
# All Address fields should roll up into parent's index
assert "$.address.address_line_1 AS address_address_line_1 TAG" in schema
assert "$.address.city AS address_city TAG" in schema
assert "$.address.state AS address_state TAG" in schema
assert "$.address.postal_code AS address_postal_code TAG" in schema
print("  [PASS] All embedded sub-fields auto-indexed")


# ── Test 5: Field(index=True) on specific embedded fields only ──────────────
banner("TEST 5: EmbeddedJsonModel with Field(index=True) on specific fields")


class Address5(EmbeddedJsonModel):
    address_line_1: str
    city: str = Field(index=True)
    state: str = Field(index=True)
    postal_code: str


class Customer5(JsonModel):
    name: str = Field(index=True)
    address: Address5 = Field(index=True)


schema = Customer5.redisearch_schema()
print(f"  schema = {schema}")
assert "$.address.city AS address_city TAG" in schema
assert "$.address.state AS address_state TAG" in schema
assert "$.address.address_line_1" not in schema, "address_line_1 NOT indexed"
assert "$.address.postal_code" not in schema, "postal_code NOT indexed"
print("  [PASS] Only marked embedded fields indexed")


# ── Test 6: Query validation respects class-level index ─────────────────────
banner("TEST 6: Query building respects class-level index")


class Customer6(JsonModel, index=True):
    first_name: str
    last_name: str


# This should work — first_name is auto-indexed via class-level index=True
q = Customer6.find(Customer6.first_name == "John")
print(f"  query = {q.query}")
assert q.query == "@first_name:{John}", f"Expected @first_name:{{John}}, got {q.query}"
print("  [PASS] Query on auto-indexed field works")


# ── Test 7: Embedded model query with class-level index ─────────────────────
banner("TEST 7: Query on embedded model field via class-level index")


class Address7(EmbeddedJsonModel, index=True):
    city: str
    state: str


class Customer7(JsonModel, index=True):
    name: str
    address: Address7


q = Customer7.find(Customer7.address.city == "San Antonio")
print(f"  query = {q.query}")
assert q.query == "@address_city:{San\\ Antonio}", f"Got: {q.query}"
print("  [PASS] Embedded query works with class-level index")


# ── Test 8: HashModel with class-level index ────────────────────────────────
banner("TEST 8: HashModel(index=True)")


class Member(HashModel, index=True):
    first_name: str
    last_name: str
    age: int


schema = Member.redisearch_schema()
print(f"  schema = {schema}")
assert "first_name TAG" in schema
assert "last_name TAG" in schema
assert "age NUMERIC" in schema
print("  [PASS] HashModel class-level index works")


# ── Test 9: Warning on too many indexed fields ──────────────────────────────
# ── Test 9: Warning on too many indexed fields ────────────────────────────────────────────────────────────────────────────────
banner("TEST 9: Warning when too many fields indexed")


# Clear the warned set to make this test deterministic
from aredis_om.model import model as _model_mod

_model_mod._class_index_warned.clear()


with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter("always")
    # The warning fires during class creation, which triggers a schema
    # generation (via ModelMeta.__new__).  Wrapping class creation in
    # ``catch_warnings`` lets us intercept it.

    class BigModel(JsonModel, index=True):
        f0: str
        f1: str
        f2: str
        f3: str
        f4: str
        f5: str
        f6: str
        f7: str
        f8: str
        f9: str
        f10: str
        f11: str
        f12: str
        f13: str
        f14: str
        f15: str
        f16: str
        f17: str
        f18: str
        f19: str
        f20: str
        f21: str
        f22: str
        f23: str
        f24: str
        f25: str

    user_warnings = [x for x in w if issubclass(x.category, UserWarning)]
    assert len(user_warnings) == 1, f"Expected 1 warning, got {len(user_warnings)}"
    msg = str(user_warnings[0].message)
    assert "27 indexed fields" in msg, f"Warning message: {msg}"
    print(f"  Warning: {msg[:80]}...")

# Verify the warning only fires once per process: subsequent schema calls
# must NOT emit again.
with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter("always")
    BigModel.redisearch_schema()
    user_warnings = [x for x in w if issubclass(x.category, UserWarning)]
    assert len(user_warnings) == 0, "Warning should fire only once per process"
print("  [PASS] Warning fires once and has correct threshold message")


# ── Test 10: Inheritance ───────────────────────────────────────────────────
banner("TEST 10: Inheritance — child inherits parent's index=True")


class BaseParent(JsonModel, index=True):
    base_field: str


class ChildModel(BaseParent):
    child_field: str


print(f"  Child._meta.index_enabled = {ChildModel._meta.index_enabled}")
schema = ChildModel.redisearch_schema()
print(f"  schema = {schema}")
assert "$.base_field AS base_field TAG" in schema
assert "$.child_field AS child_field TAG" in schema
print("  [PASS] Child inherits class-level index=True")


print("\n" + "=" * 72)
print("  ALL TESTS PASSED")
print("=" * 72)
