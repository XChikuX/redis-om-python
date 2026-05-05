# type: ignore
"""
Tests for complex embedded structures in JsonModel/HashModel.

Covers:
- Multiple EmbeddedJsonModel fields at the same level
- Deep (4-level) embedding chains
- Optional embedded models (None and set)
- List of EmbeddedJsonModel with indexed string fields
- Embedded model containing a List[str] with full_text_search
- OR / AND / NOT queries spanning multiple embedded levels
- Update operations on deeply-nested fields via __ notation
- get_many on models with embedded structures
- HashModel coexisting and interoperating with JsonModel (pk reference)
- Completely optional embedded models (all fields None)
- Embedded model with GEO coordinates inside a list
- Pipeline save with embedded models
- Empty / single-element list of embedded models
- Nested embedded re-querying after update
"""

import abc
from typing import List, Optional

import pytest
import pytest_asyncio

from aredis_om import (
    Coordinates,
    EmbeddedJsonModel,
    Field,
    HashModel,
    JsonModel,
    Migrator,
    NotFoundError,
    RedisModelError,
)
from tests._sync_redis import has_redis_json, has_redisearch

from .conftest import py_test_mark_asyncio

if not has_redis_json():
    pytestmark = pytest.mark.skip


# ---------------------------------------------------------------------------
# Shared fixture: base class + all embedded/top-level model definitions
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def em(key_prefix, redis):
    """Return a namespace of all models used in this module."""

    # ── Base classes ──────────────────────────────────────────────────────
    class BaseJson(JsonModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix

    class BaseHash(HashModel, abc.ABC):
        class Meta:
            global_key_prefix = key_prefix

    # ── Level-4 (deepest) embedded ────────────────────────────────────────
    class ContactDetails(EmbeddedJsonModel):
        """Deepest leaf: phone + email."""

        phone: str = Field(index=True)
        email: str = Field(index=True)

    # ── Level-3 embedded ─────────────────────────────────────────────────
    class Office(EmbeddedJsonModel):
        """Contains level-4 contact details."""

        building: str = Field(index=True)
        floor: int
        contact: ContactDetails

    # ── Level-2 embedded ─────────────────────────────────────────────────
    class Department(EmbeddedJsonModel):
        """Contains level-3 office."""

        name: str = Field(index=True)
        budget: float
        office: Office

    # ── Top-level JsonModel with deep embedding ───────────────────────────
    class Company(BaseJson):
        """4-level deep: Company → Department → Office → ContactDetails."""

        name: str = Field(index=True)
        industry: str = Field(index=True)
        department: Department

    # ── Multiple embedded models at the SAME level ────────────────────────
    class Skills(EmbeddedJsonModel):
        primary: str = Field(index=True)
        secondary: Optional[str] = Field(index=True, default=None)

    class Employment(EmbeddedJsonModel):
        employer: str = Field(index=True)
        role: str = Field(index=True)
        years: int

    class Education(EmbeddedJsonModel):
        institution: str = Field(index=True)
        degree: str = Field(index=True)
        graduated: int  # year

    class Profile(BaseJson):
        """Three EmbeddedJsonModel fields at the same level."""

        username: str = Field(index=True)
        bio: Optional[str] = Field(index=True, full_text_search=True, default="")
        skills: Skills
        employment: Employment
        education: Education

    # ── Optional embedded model ───────────────────────────────────────────
    class Address(EmbeddedJsonModel):
        city: str = Field(index=True)
        country: str = Field(index=True)

    class Person(BaseJson):
        name: str = Field(index=True)
        address: Optional[Address] = None

    # ── List[EmbeddedJsonModel] with indexed string field ─────────────────
    class Tag(EmbeddedJsonModel):
        label: str = Field(index=True)

    class Article(BaseJson):
        title: str = Field(index=True)
        tags: List[Tag]

    # ── EmbeddedJsonModel containing List[str] with full_text_search ──────
    class Section(EmbeddedJsonModel):
        heading: str = Field(index=True)
        keywords: List[str] = Field(index=True, full_text_search=True)

    class Document(BaseJson):
        doc_title: str = Field(index=True)
        section: Section

    # ── HashModel whose pk is referenced by a JsonModel ───────────────────
    class Tenant(BaseHash):
        tenant_name: str = Field(index=True)

    class Subscription(BaseJson):
        """Stores the HashModel pk as a plain string field."""

        tenant_pk: str = Field(index=True)
        plan: str = Field(index=True)

    # ── All-optional embedded model ───────────────────────────────────────
    class Metadata(EmbeddedJsonModel):
        note: Optional[str] = Field(index=True, default=None)
        score: Optional[float] = None

    class Widget(BaseJson):
        widget_name: str = Field(index=True)
        meta: Optional[Metadata] = None

    # ── EmbeddedJsonModel with Coordinates (GEO) ─────────────────────────
    class Venue(EmbeddedJsonModel):
        venue_name: str = Field(index=True)
        location: Optional[Coordinates] = Field(index=True, default=None)

    class Event(BaseJson):
        event_name: str = Field(index=True)
        venue: Venue

    # ── List[EmbeddedJsonModel] where items have string + GEO fields ──────
    class Stop(EmbeddedJsonModel):
        city: str = Field(index=True)

    class Route(BaseJson):
        route_name: str = Field(index=True)
        stops: List[Stop]

    await Migrator().run()

    import collections

    Models = collections.namedtuple(
        "Models",
        [
            "BaseJson",
            "BaseHash",
            "ContactDetails",
            "Office",
            "Department",
            "Company",
            "Skills",
            "Employment",
            "Education",
            "Profile",
            "Address",
            "Person",
            "Tag",
            "Article",
            "Section",
            "Document",
            "Tenant",
            "Subscription",
            "Metadata",
            "Widget",
            "Venue",
            "Event",
            "Stop",
            "Route",
        ],
    )
    return Models(
        BaseJson,
        BaseHash,
        ContactDetails,
        Office,
        Department,
        Company,
        Skills,
        Employment,
        Education,
        Profile,
        Address,
        Person,
        Tag,
        Article,
        Section,
        Document,
        Tenant,
        Subscription,
        Metadata,
        Widget,
        Venue,
        Event,
        Stop,
        Route,
    )


# ===========================================================================
# 1. Deep (4-level) embedding – save / retrieve / query
# ===========================================================================


@py_test_mark_asyncio
async def test_deep_embed_save_and_retrieve(em):
    contact = em.ContactDetails(phone="555-1234", email="ops@corp.com")
    office = em.Office(building="HQ", floor=3, contact=contact)
    dept = em.Department(name="Engineering", budget=1_000_000.0, office=office)
    company = em.Company(name="Acme", industry="Technology", department=dept)
    await company.save()

    fetched = await em.Company.get(company.pk)
    assert fetched.name == "Acme"
    assert fetched.department.name == "Engineering"
    assert fetched.department.office.building == "HQ"
    assert fetched.department.office.floor == 3
    assert fetched.department.office.contact.phone == "555-1234"
    assert fetched.department.office.contact.email == "ops@corp.com"


@py_test_mark_asyncio
async def test_deep_embed_query_on_top_level_fields(em):
    c1 = em.Company(
        name="Alpha Inc",
        industry="Finance",
        department=em.Department(
            name="Accounting",
            budget=500_000.0,
            office=em.Office(
                building="Tower A",
                floor=1,
                contact=em.ContactDetails(phone="111", email="a@a.com"),
            ),
        ),
    )
    c2 = em.Company(
        name="Beta Corp",
        industry="Technology",
        department=em.Department(
            name="R&D",
            budget=2_000_000.0,
            office=em.Office(
                building="Lab",
                floor=5,
                contact=em.ContactDetails(phone="222", email="b@b.com"),
            ),
        ),
    )
    await c1.save()
    await c2.save()

    results = await em.Company.find(em.Company.industry == "Technology").all()
    pks = {r.pk for r in results}
    assert c2.pk in pks
    assert c1.pk not in pks


@py_test_mark_asyncio
async def test_deep_embed_query_on_level2_field(em):
    c1 = em.Company(
        name="Zeta",
        industry="Health",
        department=em.Department(
            name="Billing",
            budget=300_000.0,
            office=em.Office(
                building="MedTower",
                floor=2,
                contact=em.ContactDetails(phone="999", email="z@z.com"),
            ),
        ),
    )
    await c1.save()

    results = await em.Company.find(em.Company.department.name == "Billing").all()
    pks = {r.pk for r in results}
    assert c1.pk in pks


@py_test_mark_asyncio
async def test_deep_embed_query_on_level3_field(em):
    c1 = em.Company(
        name="Sigma",
        industry="Retail",
        department=em.Department(
            name="Logistics",
            budget=750_000.0,
            office=em.Office(
                building="Warehouse",
                floor=1,
                contact=em.ContactDetails(phone="777", email="s@s.com"),
            ),
        ),
    )
    await c1.save()

    results = await em.Company.find(
        em.Company.department.office.building == "Warehouse"
    ).all()
    pks = {r.pk for r in results}
    assert c1.pk in pks


@py_test_mark_asyncio
async def test_deep_embed_query_on_level4_field(em):
    c1 = em.Company(
        name="Tau",
        industry="Media",
        department=em.Department(
            name="Editorial",
            budget=200_000.0,
            office=em.Office(
                building="Press House",
                floor=4,
                contact=em.ContactDetails(phone="333-UNIQUE", email="tau@tau.com"),
            ),
        ),
    )
    await c1.save()

    results = await em.Company.find(
        em.Company.department.office.contact.phone == "333-UNIQUE"
    ).all()
    pks = {r.pk for r in results}
    assert c1.pk in pks


# ===========================================================================
# 2. Multiple EmbeddedJsonModel fields at the same level
# ===========================================================================


@py_test_mark_asyncio
async def test_multiple_embedded_same_level_save_retrieve(em):
    skills = em.Skills(primary="Python", secondary="Rust")
    employment = em.Employment(employer="OpenAI", role="Engineer", years=3)
    education = em.Education(institution="MIT", degree="BSc", graduated=2020)

    profile = em.Profile(
        username="dev42",
        bio="Backend developer passionate about distributed systems",
        skills=skills,
        employment=employment,
        education=education,
    )
    await profile.save()

    fetched = await em.Profile.get(profile.pk)
    assert fetched.skills.primary == "Python"
    assert fetched.skills.secondary == "Rust"
    assert fetched.employment.employer == "OpenAI"
    assert fetched.employment.role == "Engineer"
    assert fetched.education.institution == "MIT"
    assert fetched.education.degree == "BSc"


@py_test_mark_asyncio
async def test_multiple_embedded_same_level_query_skills(em):
    p1 = em.Profile(
        username="rustacean",
        skills=em.Skills(primary="Rust"),
        employment=em.Employment(employer="Mozilla", role="SWE", years=2),
        education=em.Education(institution="Stanford", degree="MSc", graduated=2019),
    )
    p2 = em.Profile(
        username="pythonista",
        skills=em.Skills(primary="Python"),
        employment=em.Employment(employer="Google", role="SWE", years=5),
        education=em.Education(institution="Caltech", degree="PhD", graduated=2021),
    )
    await p1.save()
    await p2.save()

    results = await em.Profile.find(em.Profile.skills.primary == "Rust").all()
    pks = {r.pk for r in results}
    assert p1.pk in pks
    assert p2.pk not in pks


@py_test_mark_asyncio
async def test_multiple_embedded_same_level_query_employment(em):
    p1 = em.Profile(
        username="googler",
        skills=em.Skills(primary="Go"),
        employment=em.Employment(employer="Google", role="SRE", years=4),
        education=em.Education(institution="CMU", degree="BSc", graduated=2018),
    )
    p2 = em.Profile(
        username="amazonian",
        skills=em.Skills(primary="Java"),
        employment=em.Employment(employer="Amazon", role="DevOps", years=6),
        education=em.Education(institution="UW", degree="BSc", graduated=2016),
    )
    await p1.save()
    await p2.save()

    results = await em.Profile.find(em.Profile.employment.employer == "Amazon").all()
    pks = {r.pk for r in results}
    assert p2.pk in pks
    assert p1.pk not in pks


@py_test_mark_asyncio
async def test_multiple_embedded_same_level_query_education(em):
    p1 = em.Profile(
        username="oxbridge",
        skills=em.Skills(primary="C++"),
        employment=em.Employment(employer="ARM", role="HW", years=7),
        education=em.Education(institution="Oxford", degree="MEng", graduated=2017),
    )
    await p1.save()

    results = await em.Profile.find(em.Profile.education.institution == "Oxford").all()
    pks = {r.pk for r in results}
    assert p1.pk in pks


@py_test_mark_asyncio
async def test_or_query_spanning_two_embedded_models(em):
    """OR across two different EmbeddedJsonModel fields must use correct prefixes."""
    p1 = em.Profile(
        username="player1",
        skills=em.Skills(primary="Kotlin"),
        employment=em.Employment(employer="JetBrains", role="Dev", years=1),
        education=em.Education(institution="HSE", degree="BSc", graduated=2023),
    )
    p2 = em.Profile(
        username="player2",
        skills=em.Skills(primary="Swift"),
        employment=em.Employment(employer="Apple", role="iOS", years=2),
        education=em.Education(institution="UIUC", degree="BSc", graduated=2022),
    )
    await p1.save()
    await p2.save()

    # Query: primary skill is Kotlin OR employer is Apple
    results = await em.Profile.find(
        (em.Profile.skills.primary == "Kotlin")
        | (em.Profile.employment.employer == "Apple")
    ).all()
    pks = {r.pk for r in results}
    assert p1.pk in pks
    assert p2.pk in pks


@py_test_mark_asyncio
async def test_and_query_spanning_two_embedded_models(em):
    p1 = em.Profile(
        username="combo1",
        skills=em.Skills(primary="Scala"),
        employment=em.Employment(employer="Databricks", role="Eng", years=3),
        education=em.Education(institution="Berkeley", degree="MSc", graduated=2020),
    )
    p2 = em.Profile(
        username="combo2",
        skills=em.Skills(primary="Scala"),
        employment=em.Employment(employer="Snowflake", role="Eng", years=2),
        education=em.Education(institution="Yale", degree="BSc", graduated=2021),
    )
    await p1.save()
    await p2.save()

    # Both have Scala as primary but only p1 is at Databricks
    results = await em.Profile.find(
        (em.Profile.skills.primary == "Scala")
        & (em.Profile.employment.employer == "Databricks")
    ).all()
    pks = {r.pk for r in results}
    assert p1.pk in pks
    assert p2.pk not in pks


@py_test_mark_asyncio
async def test_and_query_spanning_parent_and_embedded(em):
    p1 = em.Profile(
        username="special_user",
        skills=em.Skills(primary="TypeScript"),
        employment=em.Employment(employer="Vercel", role="FE", years=2),
        education=em.Education(institution="NYU", degree="BSc", graduated=2022),
    )
    p2 = em.Profile(
        username="other_user",
        skills=em.Skills(primary="TypeScript"),
        employment=em.Employment(employer="Netlify", role="FE", years=1),
        education=em.Education(institution="NYU", degree="BSc", graduated=2022),
    )
    await p1.save()
    await p2.save()

    # username == "special_user" AND skills.primary == "TypeScript"
    results = await em.Profile.find(
        (em.Profile.username == "special_user")
        & (em.Profile.skills.primary == "TypeScript")
    ).all()
    pks = {r.pk for r in results}
    assert p1.pk in pks
    assert p2.pk not in pks


@py_test_mark_asyncio
async def test_not_query_on_embedded_field(em):
    p1 = em.Profile(
        username="not_apple",
        skills=em.Skills(primary="Elixir"),
        employment=em.Employment(employer="Discord", role="BE", years=4),
        education=em.Education(institution="RIT", degree="BSc", graduated=2019),
    )
    p2 = em.Profile(
        username="apple_employee",
        skills=em.Skills(primary="Swift"),
        employment=em.Employment(employer="Apple", role="iOS", years=3),
        education=em.Education(institution="UW", degree="BSc", graduated=2018),
    )
    await p1.save()
    await p2.save()

    results = await em.Profile.find(~(em.Profile.employment.employer == "Apple")).all()
    pks = {r.pk for r in results}
    assert p1.pk in pks
    assert p2.pk not in pks


# ===========================================================================
# 3. Optional embedded model (None vs. set)
# ===========================================================================


@py_test_mark_asyncio
async def test_optional_embedded_model_none(em):
    person = em.Person(name="NoAddress")
    await person.save()

    fetched = await em.Person.get(person.pk)
    assert fetched.name == "NoAddress"
    assert fetched.address is None


@py_test_mark_asyncio
async def test_optional_embedded_model_set(em):
    person = em.Person(
        name="WithAddress",
        address=em.Address(city="Berlin", country="Germany"),
    )
    await person.save()

    fetched = await em.Person.get(person.pk)
    assert fetched.address is not None
    assert fetched.address.city == "Berlin"
    assert fetched.address.country == "Germany"


@py_test_mark_asyncio
async def test_optional_embedded_model_query_on_city(em):
    p1 = em.Person(name="Berliner", address=em.Address(city="Berlin", country="DE"))
    p2 = em.Person(name="Parisian", address=em.Address(city="Paris", country="FR"))
    p3 = em.Person(name="NoCity")
    await p1.save()
    await p2.save()
    await p3.save()

    results = await em.Person.find(em.Person.address.city == "Berlin").all()
    pks = {r.pk for r in results}
    assert p1.pk in pks
    assert p2.pk not in pks


@py_test_mark_asyncio
async def test_optional_embedded_updated_from_none_to_set(em):
    """Set an optional embedded model that was initially None."""
    person = em.Person(name="Changeable")
    await person.save()
    assert person.address is None

    await person.update(address=em.Address(city="Rome", country="IT"))
    fetched = await em.Person.get(person.pk)
    assert fetched.address is not None
    assert fetched.address.city == "Rome"


# ===========================================================================
# 4. List of EmbeddedJsonModel
# ===========================================================================


@py_test_mark_asyncio
async def test_list_of_embedded_save_retrieve(em):
    article = em.Article(
        title="Redis OM Deep Dive",
        tags=[em.Tag(label="redis"), em.Tag(label="python"), em.Tag(label="orm")],
    )
    await article.save()

    fetched = await em.Article.get(article.pk)
    assert fetched.title == "Redis OM Deep Dive"
    assert len(fetched.tags) == 3
    labels = {t.label for t in fetched.tags}
    assert labels == {"redis", "python", "orm"}


@py_test_mark_asyncio
async def test_list_of_embedded_query_on_label(em):
    a1 = em.Article(
        title="Redis Basics",
        tags=[em.Tag(label="redis"), em.Tag(label="tutorial")],
    )
    a2 = em.Article(
        title="Python Tips",
        tags=[em.Tag(label="python"), em.Tag(label="tips")],
    )
    await a1.save()
    await a2.save()

    results = await em.Article.find(em.Article.tags.label == "redis").all()
    pks = {r.pk for r in results}
    assert a1.pk in pks
    assert a2.pk not in pks


@py_test_mark_asyncio
async def test_list_of_embedded_empty_list(em):
    """An empty list of embedded models should save and retrieve cleanly."""
    article = em.Article(title="No Tags", tags=[])
    await article.save()

    fetched = await em.Article.get(article.pk)
    assert fetched.title == "No Tags"
    assert fetched.tags == []


@py_test_mark_asyncio
async def test_list_of_embedded_single_item(em):
    article = em.Article(title="One Tag", tags=[em.Tag(label="solo")])
    await article.save()

    fetched = await em.Article.get(article.pk)
    assert len(fetched.tags) == 1
    assert fetched.tags[0].label == "solo"


# ===========================================================================
# 5. Embedded model containing List[str] with full_text_search
# ===========================================================================


@py_test_mark_asyncio
async def test_embedded_with_list_str_fts_save_retrieve(em):
    doc = em.Document(
        doc_title="Advanced Redis",
        section=em.Section(
            heading="Indexing",
            keywords=["search", "indexing", "performance"],
        ),
    )
    await doc.save()

    fetched = await em.Document.get(doc.pk)
    assert fetched.section.heading == "Indexing"
    assert set(fetched.section.keywords) == {"search", "indexing", "performance"}


@py_test_mark_asyncio
async def test_embedded_with_list_str_fts_tag_membership(em):
    doc = em.Document(
        doc_title="Tagged Doc",
        section=em.Section(heading="Intro", keywords=["hello", "world"]),
    )
    await doc.save()

    results = await em.Document.find(em.Document.section.keywords << ["hello"]).all()
    pks = {r.pk for r in results}
    assert doc.pk in pks


# ===========================================================================
# 6. HashModel coexisting with JsonModel; pk reference pattern
# ===========================================================================


@pytest.mark.skipif(not has_redisearch(), reason="requires RediSearch")
@py_test_mark_asyncio
async def test_hash_and_json_coexist_in_same_key_prefix(em):
    """HashModel and JsonModel should operate independently under the same prefix."""
    tenant = em.Tenant(tenant_name="AcmeCorp")
    await tenant.save()

    sub = em.Subscription(tenant_pk=str(tenant.pk), plan="enterprise")
    await sub.save()

    fetched_tenant = await em.Tenant.get(tenant.pk)
    fetched_sub = await em.Subscription.get(sub.pk)

    assert fetched_tenant.tenant_name == "AcmeCorp"
    assert fetched_sub.plan == "enterprise"
    assert fetched_sub.tenant_pk == str(tenant.pk)


@pytest.mark.skipif(not has_redisearch(), reason="requires RediSearch")
@py_test_mark_asyncio
async def test_json_find_by_hash_model_pk_reference(em):
    """Query a JsonModel by the pk of a referenced HashModel stored as a string."""
    tenant = em.Tenant(tenant_name="Foo Inc")
    await tenant.save()

    sub = em.Subscription(tenant_pk=str(tenant.pk), plan="basic")
    await sub.save()

    results = await em.Subscription.find(
        em.Subscription.tenant_pk == str(tenant.pk)
    ).all()
    assert len(results) == 1
    assert results[0].plan == "basic"


# ===========================================================================
# 7. All-optional embedded model
# ===========================================================================


@py_test_mark_asyncio
async def test_all_optional_embedded_none(em):
    widget = em.Widget(widget_name="bare")
    await widget.save()

    fetched = await em.Widget.get(widget.pk)
    assert fetched.widget_name == "bare"
    assert fetched.meta is None


@py_test_mark_asyncio
async def test_all_optional_embedded_partial(em):
    widget = em.Widget(
        widget_name="partial", meta=em.Metadata(note="important", score=None)
    )
    await widget.save()

    fetched = await em.Widget.get(widget.pk)
    assert fetched.meta is not None
    assert fetched.meta.note == "important"
    assert fetched.meta.score is None


@py_test_mark_asyncio
async def test_all_optional_embedded_full(em):
    widget = em.Widget(widget_name="full", meta=em.Metadata(note="hello", score=9.5))
    await widget.save()

    fetched = await em.Widget.get(widget.pk)
    assert fetched.meta.note == "hello"
    assert abs(fetched.meta.score - 9.5) < 1e-6


# ===========================================================================
# 8. Embedded model with GEO (Coordinates)
# ===========================================================================


@py_test_mark_asyncio
async def test_embedded_with_geo_save_retrieve(em):
    event = em.Event(
        event_name="Tech Summit",
        venue=em.Venue(
            venue_name="Convention Center",
            location=Coordinates(latitude=37.7749, longitude=-122.4194),
        ),
    )
    await event.save()

    fetched = await em.Event.get(event.pk)
    assert fetched.event_name == "Tech Summit"
    assert fetched.venue.venue_name == "Convention Center"
    assert fetched.venue.location is not None


@py_test_mark_asyncio
async def test_embedded_with_geo_none_location(em):
    event = em.Event(
        event_name="Virtual Event",
        venue=em.Venue(venue_name="Online", location=None),
    )
    await event.save()

    fetched = await em.Event.get(event.pk)
    assert fetched.venue.location is None


# ===========================================================================
# 9. List[EmbeddedJsonModel] with city field
# ===========================================================================


@py_test_mark_asyncio
async def test_route_list_stops_save_retrieve(em):
    route = em.Route(
        route_name="West Coast",
        stops=[em.Stop(city="LA"), em.Stop(city="SF"), em.Stop(city="Seattle")],
    )
    await route.save()

    fetched = await em.Route.get(route.pk)
    assert fetched.route_name == "West Coast"
    assert len(fetched.stops) == 3
    cities = {s.city for s in fetched.stops}
    assert cities == {"LA", "SF", "Seattle"}


@py_test_mark_asyncio
async def test_route_list_query_on_stop_city(em):
    r1 = em.Route(
        route_name="US Route",
        stops=[em.Stop(city="NYC"), em.Stop(city="Boston")],
    )
    r2 = em.Route(
        route_name="EU Route",
        stops=[em.Stop(city="Paris"), em.Stop(city="Berlin")],
    )
    await r1.save()
    await r2.save()

    results = await em.Route.find(em.Route.stops.city == "Paris").all()
    pks = {r.pk for r in results}
    assert r2.pk in pks
    assert r1.pk not in pks


# ===========================================================================
# 10. Update operations on embedded fields via __ path notation
# ===========================================================================


@py_test_mark_asyncio
async def test_update_embedded_field_via_double_underscore(em):
    profile = em.Profile(
        username="updatable",
        skills=em.Skills(primary="Java"),
        employment=em.Employment(employer="Oracle", role="Dev", years=10),
        education=em.Education(institution="IIT", degree="BTech", graduated=2013),
    )
    await profile.save()

    await profile.update(employment__employer="SAP")
    fetched = await em.Profile.get(profile.pk)
    assert fetched.employment.employer == "SAP"


@py_test_mark_asyncio
async def test_update_deep_embedded_field(em):
    contact = em.ContactDetails(phone="000", email="before@x.com")
    office = em.Office(building="Old", floor=1, contact=contact)
    dept = em.Department(name="IT", budget=100_000.0, office=office)
    company = em.Company(name="Updatable Corp", industry="Finance", department=dept)
    await company.save()

    # Update the nested contact email
    await company.update(department__office__contact__email="after@x.com")
    fetched = await em.Company.get(company.pk)
    assert fetched.department.office.contact.email == "after@x.com"


@py_test_mark_asyncio
async def test_update_optional_embedded_to_new_value(em):
    person = em.Person(name="UpdateMe")
    await person.save()
    assert person.address is None

    await person.update(address=em.Address(city="Madrid", country="ES"))
    fetched = await em.Person.get(person.pk)
    assert fetched.address.city == "Madrid"

    await person.update(address__city="Barcelona")
    fetched2 = await em.Person.get(person.pk)
    assert fetched2.address.city == "Barcelona"
    assert fetched2.address.country == "ES"


# ===========================================================================
# 11. get_many with embedded structures
# ===========================================================================


@py_test_mark_asyncio
async def test_get_many_with_embedded(em):
    profiles = []
    for i in range(5):
        p = em.Profile(
            username=f"bulk_user_{i}",
            skills=em.Skills(primary=f"Lang{i}"),
            employment=em.Employment(employer=f"Employer{i}", role="Dev", years=i),
            education=em.Education(
                institution=f"Uni{i}", degree="BSc", graduated=2020 + i
            ),
        )
        await p.save()
        profiles.append(p)

    pks = [p.pk for p in profiles]
    results = await em.Profile.get_many(pks)
    assert len(results) == 5
    usernames = {r.username for r in results}
    assert usernames == {f"bulk_user_{i}" for i in range(5)}


# ===========================================================================
# 12. Pipeline save with embedded models
# ===========================================================================


@py_test_mark_asyncio
async def test_pipeline_save_with_embedded(em):
    pipeline = em.Profile.Meta.database.pipeline()

    p1 = em.Profile(
        username="pipe_user_1",
        skills=em.Skills(primary="Ruby"),
        employment=em.Employment(employer="Shopify", role="BE", years=4),
        education=em.Education(institution="Waterloo", degree="BSc", graduated=2019),
    )
    p2 = em.Profile(
        username="pipe_user_2",
        skills=em.Skills(primary="PHP"),
        employment=em.Employment(employer="WordPress", role="FE", years=3),
        education=em.Education(institution="Toronto", degree="BSc", graduated=2020),
    )
    await p1.save(pipeline=pipeline)
    await p2.save(pipeline=pipeline)
    await pipeline.execute()

    fetched1 = await em.Profile.get(p1.pk)
    fetched2 = await em.Profile.get(p2.pk)
    assert fetched1.username == "pipe_user_1"
    assert fetched2.username == "pipe_user_2"
    assert fetched1.skills.primary == "Ruby"
    assert fetched2.skills.primary == "PHP"


# ===========================================================================
# 13. Schema correctness assertions
# ===========================================================================


def test_multiple_embedded_schema_contains_all_prefixed_fields(em):
    schema = em.Profile.redisearch_schema()
    # Each embedded model should have its own prefixed index entries
    assert "$.skills.primary" in schema
    assert "$.employment.employer" in schema
    assert "$.employment.role" in schema
    assert "$.education.institution" in schema
    assert "$.education.degree" in schema


def test_deep_embed_schema_contains_level4_paths(em):
    schema = em.Company.redisearch_schema()
    assert "$.department.name" in schema
    assert "$.department.office.building" in schema
    assert "$.department.office.contact.phone" in schema
    assert "$.department.office.contact.email" in schema


# ===========================================================================
# 14. Edge case: saving the same model instance twice (idempotent update)
# ===========================================================================


@py_test_mark_asyncio
async def test_save_twice_preserves_final_state(em):
    person = em.Person(name="Dup", address=em.Address(city="Oslo", country="NO"))
    await person.save()
    person.name = "DupUpdated"
    person.address.city = "Bergen"
    await person.save()

    fetched = await em.Person.get(person.pk)
    assert fetched.name == "DupUpdated"
    assert fetched.address.city == "Bergen"


# ===========================================================================
# 15. Edge case: two separate models with identically-named embedded fields
#     should not cross-contaminate queries (regression for prefix-sharing bug)
# ===========================================================================


@py_test_mark_asyncio
async def test_or_query_same_field_name_two_embedded_models(em):
    """
    Profile.skills.primary and Profile.employment.role are different fields
    but both named 'primary'/'role' in their respective embedded models.
    An OR query should produce the correct per-model prefixes.
    """
    p1 = em.Profile(
        username="prefix_test_1",
        skills=em.Skills(primary="Go"),
        employment=em.Employment(employer="HashiCorp", role="SRE", years=5),
        education=em.Education(institution="McGill", degree="BSc", graduated=2018),
    )
    p2 = em.Profile(
        username="prefix_test_2",
        skills=em.Skills(primary="Rust"),
        employment=em.Employment(employer="Cloudflare", role="Network", years=3),
        education=em.Education(institution="McGill", degree="BSc", graduated=2019),
    )
    await p1.save()
    await p2.save()

    # OR on two unrelated embedded fields
    results = await em.Profile.find(
        (em.Profile.skills.primary == "Go")
        | (em.Profile.employment.employer == "Cloudflare")
    ).all()
    pks = {r.pk for r in results}
    assert p1.pk in pks
    assert p2.pk in pks


# ===========================================================================
# 16. HashModel basic CRUD (sanity check alongside JsonModel tests)
# ===========================================================================


@pytest.mark.skipif(not has_redisearch(), reason="requires RediSearch")
@py_test_mark_asyncio
async def test_hash_model_basic_crud(em):
    t1 = em.Tenant(tenant_name="Tenant A")
    await t1.save()

    fetched = await em.Tenant.get(t1.pk)
    assert fetched.tenant_name == "Tenant A"

    t1.tenant_name = "Tenant A Updated"
    await t1.save()

    updated = await em.Tenant.get(t1.pk)
    assert updated.tenant_name == "Tenant A Updated"

    await em.Tenant.delete(t1.pk)
    with pytest.raises(NotFoundError):
        await em.Tenant.get(t1.pk)


@pytest.mark.skipif(not has_redisearch(), reason="requires RediSearch")
@py_test_mark_asyncio
async def test_hash_model_query_alongside_json_model(em):
    """Both models share the same Redis instance; queries must be isolated."""
    t1 = em.Tenant(tenant_name="QueryTenantX")
    await t1.save()

    sub = em.Subscription(tenant_pk=str(t1.pk), plan="pro")
    await sub.save()

    # Searching for the HashModel by its own field
    hash_results = await em.Tenant.find(em.Tenant.tenant_name == "QueryTenantX").all()
    assert any(r.pk == t1.pk for r in hash_results)

    # Searching for the JsonModel by the referenced pk string
    json_results = await em.Subscription.find(
        em.Subscription.tenant_pk == str(t1.pk)
    ).all()
    assert any(r.pk == sub.pk for r in json_results)
