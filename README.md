<div align="center">
  <br/>
  <br/>
  <img width="360" src="https://raw.githubusercontent.com/redis-developer/redis-om-python/main/images/logo.svg?token=AAAXXHUYL6RHPESRRAMBJOLBSVQXE" alt="Redis OM" />
  <br/>
  <br/>
</div>

<p align="center">
    <p align="center">
        Object mapping, and more, for Redis and Python
    </p>
</p>

---

[![Version][version-svg]][package-url]
[![License][license-image]][license-url]
[![Build Status][ci-svg]][ci-url]

**Redis OM Python** makes it easy to model Redis data in your Python applications.

<details>
  <summary><strong>Table of contents</strong></summary>

span

<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [💡 Why Redis OM?](#-why-redis-om)
- [💻 Installation](#-installation)
- [🏁 Getting started](#-getting-started)
- [📇 Modeling Your Data](#-modeling-your-data)
- [✓ Validating Data With Your Model](#-validating-data-with-your-model)
- [🔎 Rich Queries and Embedded Models](#-rich-queries-and-embedded-models)
  - [Querying](#querying)
  - [Embedded Models](#embedded-models)
  - [GEO Spatial Queries](#geo-spatial-queries)
- [Calling Other Redis Commands](#calling-other-redis-commands)
- [📚 Documentation](#-documentation)
- [⛏️ Troubleshooting](#️-troubleshooting)
- [✨ So How Do You Get RediSearch and RedisJSON?](#-so-how-do-you-get-redisearch-and-redisjson)
- [❤️ Contributing](#️-contributing)
- [📝 License](#-license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

</details>

## 💡 Why Redis OM?

Redis OM provides high-level abstractions that make it easy to model and query data in Redis with modern Python applications.

This **preview** release contains the following features:

- Declarative object mapping for Redis objects
- Declarative secondary-index generation
- Fluent APIs for querying Redis

## 💻 Installation

Installation is simple with `pip`, Poetry, or Pipenv.

```sh
# With pip
$ pip install redis-om

# Or, using Poetry
$ poetry add redis-om
```

## 🏁 Getting started

### Starting Redis

Before writing any code you'll need a Redis instance with the appropriate Redis modules! The quickest way to get this is with Docker:

```sh
docker run -p 6379:6379 -p 8001:8001 redis/redis-stack
```

This launches the [redis-stack](https://redis.io/docs/stack/) an extension of Redis that adds all manner of modern data structures to Redis. You'll also notice that if you open up http://localhost:8001 you'll have access to the redis-insight GUI, a GUI you can use to visualize and work with your data in Redis.

## 📇 Modeling Your Data

Redis OM contains powerful declarative models that give you data validation, serialization, and persistence to Redis.

Check out this example of modeling customer data with Redis OM. First, we create a `Customer` model:

```python
import datetime
from typing import Optional

from pydantic import EmailStr

from redis_om import HashModel


class Customer(HashModel):
    first_name: str
    last_name: str
    email: EmailStr
    join_date: datetime.date
    age: int
    bio: Optional[str]
```

Now that we have a `Customer` model, let's use it to save customer data to Redis.

```python
import datetime
from typing import Optional
from pydantic import EmailStr

from aredis_om import (
    Field,
    HashModel,
    JsonModel,
    EmbeddedJsonModel,
    Migrator,
    get_redis_connection,
)
from aredis_om.model.model import NotFoundError

redis_conn = get_redis_connection(
    url="redis://10.9.9.100:6379",
    decode_responses=False,
    password="D1ngD0ng",

class Customer(HashModel):
    first_name: str
    last_name: str
    email: EmailStr
    join_date: datetime.date
    age: int
    bio: Optional[str]

    class Meta:
        database = redis_conn

    class Config:
        # Ensure that updates will undergo validation by pydantic
        validate_assignment = True
        anystr_strip_whitespace = True


# First, we create a new `Customer` object:
andrew = Customer(
    first_name="Andrew",
    last_name="Brookins",
    email="andrew.brookins@example.com",
    join_date=datetime.date.today(),
    age=38,
    bio="Python developer, works at Redis, Inc."
)

# The model generates a globally unique primary key automatically
# without needing to talk to Redis.
print(andrew.pk)
# > "01FJM6PH661HCNNRC884H6K30C"

# We can save the model to Redis by calling `save()`:
andrew.save()

# Expire the model after 2 mins (120 seconds)
andrew.expire(120)

# To retrieve this customer with its primary key, we use `Customer.get()`:
assert Customer.get(andrew.pk) == andrew
```

**Ready to learn more?** Check out the [getting started](docs/getting_started.md) guide.

Or, continue reading to see how Redis OM makes data validation a snap.

## ✓ Validating Data With Your Model

Redis OM uses [Pydantic][pydantic-url] to validate data based on the type annotations you assign to fields in a model class.

This validation ensures that fields like `first_name`, which the `Customer` model marked as a `str`, are always strings. **But every Redis OM model is also a Pydantic model**, so you can use Pydantic validators like `EmailStr`, `Pattern`, and many more for complex validations!

For example, because we used the `EmailStr` type for the `email` field, we'll get a validation error if we try to create a `Customer` with an invalid email address:

```python
import datetime
from typing import Optional

from pydantic import EmailStr, ValidationError

from redis_om import HashModel


class Customer(HashModel):
    first_name: str
    last_name: str
    email: EmailStr
    join_date: datetime.date
    age: int
    bio: Optional[str]


try:
    Customer(
        first_name="Andrew",
        last_name="Brookins",
        email="Not an email address!",
        join_date=datetime.date.today(),
        age=38,
        bio="Python developer, works at Redis, Inc."
    )
except ValidationError as e:
    print(e)
    """
    pydantic.error_wrappers.ValidationError: 1 validation error for Customer
     email
       value is not a valid email address (type=value_error.email)
    """
```

**Any existing Pydantic validator should work** as a drop-in type annotation with a Redis OM model. You can also write arbitrarily complex custom validations!

To learn more, see the [documentation on data validation](docs/validation.md).

## 🔎 Rich Queries and Embedded Models

Data modeling, validation, and saving models to Redis all work regardless of how you run Redis.

Next, we'll show you the **rich query expressions** and **embedded models** Redis OM provides when the [RediSearch][redisearch-url] and [RedisJSON][redis-json-url] modules are installed in your Redis deployment, or you're using [Redis Enterprise][redis-enterprise-url].

**TIP**: _Wait, what's a Redis module?_ If you aren't familiar with Redis modules, review the [So, How Do You Get RediSearch and RedisJSON?](#-so-how-do-you-get-redisearch-and-redisjson) section of this README.

### Querying

Redis OM comes with a rich query language that allows you to query Redis with Python expressions.

To show how this works, we'll make a small change to the `Customer` model we defined earlier. We'll add `Field(index=True)` to tell Redis OM that we want to index the `last_name` and `age` fields:

```python
import datetime
from typing import Optional

from pydantic import EmailStr

from redis_om import (
    Field,
    HashModel,
    Migrator
)


class Customer(HashModel):
    first_name: str
    last_name: str = Field(index=True)
    email: EmailStr
    join_date: datetime.date
    age: int = Field(index=True)
    bio: Optional[str]


# Now, if we use this model with a Redis deployment that has the
# RediSearch module installed, we can run queries like the following.

# Before running queries, we need to run migrations to set up the
# indexes that Redis OM will use. You can also use the `migrate`
# CLI tool for this!
Migrator().run()

# Find all customers with the last name "Brookins"
Customer.find(Customer.last_name == "Brookins").all()

# Find all customers that do NOT have the last name "Brookins"
Customer.find(Customer.last_name != "Brookins").all()

# Find all customers whose last name is "Brookins" OR whose age is
# 100 AND whose last name is "Smith"
Customer.find((Customer.last_name == "Brookins") | (
        Customer.age == 100
) & (Customer.last_name == "Smith")).all()
```

These queries -- and more! -- are possible because **Redis OM manages indexes for you automatically**.

Querying with this index features a rich expression syntax inspired by the Django ORM, SQLAlchemy, and Peewee. We think you'll enjoy it!

**Note:** Indexing only works for data stored in Redis logical database 0. If you are using a different database number when connecting to Redis, you can expect the code to raise a `MigrationError` when you run the migrator.

### Embedded Models

Redis OM can store and query **nested models** like any document database, with the speed and power you get from Redis. Let's see how this works.

In the next example, we'll define a new `Address` model and embed it within the `Customer` model.

```python
import datetime
from typing import Optional

from redis_om import (
    EmbeddedJsonModel,
    JsonModel,
    Field,
    Migrator,
)


class Address(EmbeddedJsonModel):
    address_line_1: str
    address_line_2: Optional[str]
    city: str = Field(index=True)
    state: str = Field(index=True)
    country: str
    postal_code: str = Field(index=True)


class Customer(JsonModel):
    first_name: str = Field(index=True)
    last_name: str = Field(index=True)
    email: str = Field(index=True)
    join_date: datetime.date
    age: int = Field(index=True)
    bio: Optional[str] = Field(index=True, full_text_search=True,
                               default="")

    # Creates an embedded model.
    address: Address


# With these two models and a Redis deployment with the RedisJSON
# module installed, we can run queries like the following.

# Before running queries, we need to run migrations to set up the
# indexes that Redis OM will use. You can also use the `migrate`
# CLI tool for this!
Migrator().run()

# Find all customers who live in San Antonio, TX
Customer.find(Customer.address.city == "San Antonio",
              Customer.address.state == "TX")
```

### GEO Spatial Queries

Redis OM supports geospatial queries through the `Coordinates` field type and `GeoFilter` for location-based searches. This is perfect for applications that need to find nearby locations, restaurants, stores, or any other location-based data.

#### Defining Models with Coordinates

First, let's create models that include geographic coordinates:

```python
import datetime
from typing import Optional

from redis_om import (
    Coordinates,
    GeoFilter,
    Field,
    HashModel,
    JsonModel,
    Migrator,
)

# Using HashModel for simple location storage
class Store(HashModel):
    name: str = Field(index=True)
    coordinates: Coordinates = Field(index=True)
    category: str = Field(index=True)

# Using JsonModel for more complex location data
class Restaurant(JsonModel):
    name: str = Field(index=True)
    coordinates: Coordinates = Field(index=True)
    cuisine: str = Field(index=True)
    rating: float = Field(index=True)
    address: str
    phone: Optional[str] = None

# Run migrations to create indexes
Migrator().run()
```

#### Storing Location Data

You can create coordinates using latitude and longitude values:

```python
# Create some stores with coordinates (latitude, longitude)
starbucks = Store(
    name="Starbucks Downtown",
    coordinates=(40.7589, -73.9851),  # Times Square, NYC
    category="Coffee"
)

whole_foods = Store(
    name="Whole Foods Market",
    coordinates=(40.7505, -73.9934),  # Near Herald Square, NYC
    category="Grocery"
)

# Save the stores
starbucks.save()
whole_foods.save()

# Create restaurants
pizza_place = Restaurant(
    name="Joe's Pizza",
    coordinates=(40.7484, -73.9857),  # Greenwich Village, NYC
    cuisine="Italian",
    rating=4.5,
    address="7 Carmine St, New York, NY 10014"
)

sushi_bar = Restaurant(
    name="Sushi Yasaka",
    coordinates=(40.7282, -74.0776),  # West Village, NYC
    cuisine="Japanese",
    rating=4.8,
    address="251 W 72nd St, New York, NY 10023"
)

pizza_place.save()
sushi_bar.save()
```

#### Querying by Location

Now you can search for locations within a specific radius using `GeoFilter`:

```python
# Find all stores within 1 mile of Times Square
times_square = (40.7589, -73.9851)

nearby_stores = Store.find(
    Store.coordinates == GeoFilter(
        longitude=times_square[1], 
        latitude=times_square[0], 
        radius=1, 
        unit="mi"
    )
).all()

print(f"Found {len(nearby_stores)} stores within 1 mile of Times Square")
for store in nearby_stores:
    print(f"- {store.name} ({store.category})")

# Find restaurants within 2 kilometers of a specific location
central_park = (40.7812, -73.9665)

nearby_restaurants = Restaurant.find(
    Restaurant.coordinates == GeoFilter(
        longitude=central_park[1],
        latitude=central_park[0],
        radius=2,
        unit="km"  # Can use 'mi', 'km', 'm', or 'ft'
    )
).all()

for restaurant in nearby_restaurants:
    print(f"{restaurant.name} - {restaurant.cuisine} cuisine, rated {restaurant.rating}")
```

#### Combining GEO Queries with Other Filters

You can combine geospatial queries with other field filters:

```python
# Find highly-rated Italian restaurants within 5 miles of downtown NYC
downtown_nyc = (40.7831, -73.9712)

good_italian_nearby = Restaurant.find(
    (Restaurant.coordinates == GeoFilter(
        longitude=downtown_nyc[1],
        latitude=downtown_nyc[0],
        radius=5,
        unit="mi"
    )) & 
    (Restaurant.cuisine == "Italian") &
    (Restaurant.rating >= 4.0)
).all()

# Find coffee shops within walking distance (0.25 miles)
walking_distance_coffee = Store.find(
    (Store.coordinates == GeoFilter(
        longitude=-73.9851,
        latitude=40.7589,
        radius=0.25,
        unit="mi"
    )) &
    (Store.category == "Coffee")
).all()
```

#### Supported Distance Units

The `GeoFilter` supports the following distance units:
- `"mi"` - Miles
- `"km"` - Kilometers  
- `"m"` - Meters
- `"ft"` - Feet

#### Advanced Location Examples

Here are some practical examples for common geospatial use cases:

```python
# Find the closest store to a user's location
user_location = (40.7500, -73.9900)

closest_stores = Store.find(
    Store.coordinates == GeoFilter(
        longitude=user_location[1],
        latitude=user_location[0],
        radius=10,  # Start with a reasonable radius
        unit="mi"
    )
).all()

if closest_stores:
    print(f"Closest store: {closest_stores[0].name}")

# Create a store locator function
def find_nearby_locations(lat: float, lon: float, radius: float = 5.0, 
                         category: Optional[str] = None):
    """Find stores within a radius, optionally filtered by category."""
    conditions = [
        Store.coordinates == GeoFilter(
            longitude=lon,
            latitude=lat,
            radius=radius,
            unit="mi"
        )
    ]
    
    if category:
        conditions.append(Store.category == category)
    
    return Store.find(*conditions).all()

# Usage examples
nearby_grocery = find_nearby_locations(40.7589, -73.9851, 2.0, "Grocery")
coffee_shops = find_nearby_locations(40.7589, -73.9851, 0.5, "Coffee")
all_nearby = find_nearby_locations(40.7589, -73.9851, 1.0)
```

## Calling Other Redis Commands

Sometimes you'll need to run a Redis command directly. Redis OM supports this through the `db` method on your model's class. This returns a connected Redis client instance which exposes a function named for each Redis command. For example, let's perform some basic set operations:

```python
from redis_om import HashModel

class Demo(HashModel):
    some_field: str

redis_conn = Demo.db()

redis_conn.sadd("myset", "a", "b", "c", "d")

# Prints False
print(redis_conn.sismember("myset", "e"))

# Prints True
print(redis_conn.sismember("myset", "b"))
```

The parameters expected by each command function are those documented on the command's page on [redis.io](https://redis.io/commands/).

If you don't want to get a Redis connection from a model class, you can also use `get_redis_connection`:

```python
from redis_om import get_redis_connection

redis_conn = get_redis_connection()
redis_conn.set("hello", "world")
```

## 🧩 Composing dynamic queries (optional conditions)

Often you’ll want to add filters only when an input is provided (e.g., building a search form). You can compose a list of expressions and splat them into `find()`.

Example:

```python
from typing import Optional, Sequence
import datetime
from pydantic import EmailStr

from redis_om import Field, HashModel, Migrator

class User(HashModel):
    first_name: str = Field(index=True)
    last_name: str = Field(index=True)
    email: EmailStr
    join_date: datetime.date
    age: int = Field(index=True, sortable=True)
    city: Optional[str] = Field(index=True)
    bio: Optional[str] = Field(index=True, full_text_search=True, default="")

# Ensure indexes exist (when using RediSearch)
Migrator().run()

def search_users(
    min_age: Optional[int] = None,
    max_age: Optional[int] = None,
    city: Optional[str] = None,
    name_prefix: Optional[str] = None,
    bio_term: Optional[str] = None,
) -> Sequence[User]:
    conditions = []

    # Range example (AND relationship)
    if min_age is not None:
        conditions.append(User.age >= min_age)
    if max_age is not None:
        conditions.append(User.age <= max_age)

    # Exact match example
    if city:
        conditions.append(User.city == city)

    # Prefix and full-text examples
    if name_prefix:
        # Uses TAG-prefix query semantics
        conditions.append(User.last_name.startswith(name_prefix))
    if bio_term:
        # Full text search (requires full_text_search=True on the field)
        conditions.append(User.bio % bio_term)

    # All conditions passed to find() are combined with logical AND.
    # For OR groups, compose an OR expression and add that single expression.
    # Example OR group: last_name startswith("Sm") OR startswith("St")
    # or_group = (User.last_name.startswith("Sm")) | (User.last_name.startswith("St"))
    # conditions.append(or_group)

    # Execute the query; you can also chain sort_by, page, etc.
    return User.find(*conditions).sort_by("-age").all()
```

Notes:
- Omit conditions by not appending them; the resulting `find(*conditions)` only includes what you provided.
- Conditions passed separately to `find()` are ANDed together. Use `|` (OR) or `~(...)` (NOT) to build grouped expressions when needed, then append that single expression.
- You can paginate with `.page(offset, limit)` or fetch the first match with `.first()`.

## 📚 Documentation

The Redis OM documentation is available [here](docs/index.md).

## ⛏️ Troubleshooting

If you run into trouble or have any questions, we're here to help!

Hit us up on the [Redis Discord Server](http://discord.gg/redis) or [open an issue on GitHub](https://github.com/redis-developer/redis-om-python/issues/new).

## ❤️ Contributing

We'd love your contributions!

**Bug reports** are especially helpful at this stage of the project. [You can open a bug report on GitHub](https://github.com/XChikuX/redis-om-python/issues/new).

You can also **contribute documentation** -- or just let us know if something needs more detail. [Open an issue on GitHub](https://github.com/XChikuX/redis-om-python/issues/new) to get started.

## 📝 License

Redis OM uses the [MIT license][license-url].

<!-- Badges -->

[version-svg]: https://img.shields.io/pypi/v/redis-om?style=flat-square
[package-url]: https://pypi.org/project/redis-om/
[ci-svg]: https://github.com/redis/redis-om-python/actions/workflows/ci.yml/badge.svg
[ci-url]: https://github.com/redis/redis-om-python/actions/workflows/ci.yml
[license-image]: https://img.shields.io/badge/license-mit-green.svg?style=flat-square
[license-url]: LICENSE

<!-- Links -->

[redis-om-website]: https://developer.redis.com
[redisearch-url]: https://redis.io/docs/stack/search/
[redis-json-url]: https://redis.io/docs/stack/json/
[pydantic-url]: https://github.com/samuelcolvin/pydantic
[ulid-url]: https://github.com/ulid/spec
