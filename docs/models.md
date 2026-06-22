# Models and Fields

The heart of Redis OM's object mapping, validation, and querying features is a
pair of declarative models: `HashModel` and `JsonModel`. Both models work
provide roughly the same API, but they store data in Redis differently.

This page will explain how to create your Redis OM model by subclassing one of
these classes.

## HashModel vs. JsonModel

First, which should you use?

The choice is relatively simple. If you want to embed a model inside another
model, like giving a `Customer` model a list of `Order` models, then you need to
use `JsonModel`. Only `JsonModel` supports embedded models.

Otherwise, use `HashModel`.

## Creating Your Model

You create a Redis OM model by subclassing `HashModel` or `JsonModel`. For
example:

```python
from redis_om import HashModel


class Customer(HashModel):
    first_name: str
    last_name: str
```

## Configuring Models

There are several Redis OM-specific settings you can configure in models. You
configure these settings using a special object called the _Meta object_.

Here is an example of using the Meta object to set a global key prefix:

```python
from redis_om import HashModel


class Customer(HashModel):
    first_name: str
    last_name: str

    class Meta:
        global_key_prefix = "customer-dashboard"
```

## Abstract Models

You can create abstract Redis OM models by subclassing `ABC` in addition to
either `HashModel` or `JsonModel`. Abstract models exist only to gather shared
configuration for subclasses -- you can't instantiate them.

One use of abstract models is to configure a Redis key prefix that all models in
your application will use. This is a good best practice with Redis. Here's how
you'd do it with an abstract model:

```python
from abc import ABC

from redis_om import HashModel


class BaseModel(HashModel, ABC):
    class Meta:
        global_key_prefix = "your-application"
```

### The Meta Object Is "Special"

The Meta object has a special property: if you create a model subclass from a base class that has a Meta object, Redis OM copies the parent's fields into the Meta object in the child class.

Because of this, a subclass can override a single field in its parent's Meta class without having to redefine all fields.

An example will make this clearer:

```python
from abc import ABC

from redis_om import HashModel, get_redis_connection


redis = get_redis_connection(port=6380)
other_redis = get_redis_connection(port=6381)


class BaseModel(HashModel, ABC):
    class Meta:
        global_key_prefix = "customer-dashboard"
        database = redis


class Customer(BaseModel):
    first_name: str
    last_name: str

    class Meta:
        database = other_redis


print(Customer.global_key_prefix)
# > "customer-dashboard"
```

In this example, we created an abstract base model called `BaseModel` and gave it a Meta object containing a database connection and a global key prefix.

Then we created a subclass `BaseModel` called `Customer` and gave it a second Meta object, but only defined `database`. `Customer` _also gets the global key prefix_ that `BaseModel` defined ("customer-dashboard").

While this is not how object inheritance usually works in Python, we think it is helpful to make abstract models more useful, especially as a way to group shared model settings.

### All Settings Supported by the Meta Object

Here is a table of the settings available in the Meta object and what they control.

| Setting                 | Description                                                                                                                                                                                                                                                 | Default                                                         |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------- |
| global_key_prefix       | A string prefix applied to every Redis key that the model manages. This could be something like your application's name.                                                                                                                                    | ""                                                              |
| model_key_prefix        | A string prefix applied to the Redis key representing every model. For example, the Redis Hash key for a HashModel. This prefix is also added to the redisearch index created for every model with indexed fields.                                          | f"{new_class.__module__}.{new_class.__name__}"                                                              |
| primary_key_pattern     | A format string producing the base string for a Redis key representing this model. This string should accept a "pk" format argument. **Note:** This is a "new style" format string, which will be called with `.format()`.                                  | "{pk}"                                                           |
| database                | A redis.asyncio.Redis or redis.Redis client instance that the model will use to communicate with Redis.                                                                                                                                                         | A new instance created with connections.get_redis_connection(). |
| primary_key_creator_cls | A class that adheres to the PrimaryKeyCreator protocol, which Redis OM will use to create a primary key for a new model instance.                                                                                                                           | UlidPrimaryKey                                                  |
| index_name              | The RediSearch index name to use for this model. Only used if at least one of the model's fields are marked as indexable (`index=True`).                                                                                                                    | "{global_key_prefix}:{model_key_prefix}:index"                  |
| embedded                | Whether or not this model is "embedded." Embedded models are not included in migrations that create and destroy indexes. Instead, their indexed fields are included in the index for the parent model. **Note**: Only `JsonModel` can have embedded models. | False                                                           |
| encoding                | The default encoding to use for strings. This encoding is given to redis-py at the connection level. In both cases, Redis OM will decode binary strings from Redis using your chosen encoding.                                                  | "utf-8"                                                         |
| default_ttl             | A default TTL (in seconds) applied to every saved model instance. When set, `save()` automatically calls `EXPIRE` on the Redis key with this value. Can be overridden per-instance by passing `expire=` to the constructor. Use `None` to disable.        | None                                                            |
## Configuring Pydantic

Every Redis OM model is also a Pydantic model, so in addition to configuring Redis OM behavior with the Meta object, you can control Pydantic configuration via the `model_config` object within a model class.

See the [Pydantic documentation for details](https://docs.pydantic.dev/latest/concepts/config/) on how this object works and the settings that are available.

The default Pydantic configuration for models, which Redis OM sets for you, is equivalent to the following (demonstrated on an actual model):

```python
from redis_om import HashModel
from pydantic import ConfigDict


class Customer(HashModel):
    # ... Fields ...

    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True,
        extra="allow",
    )
```

Some features may not work correctly if you change these settings.

## Fields

You define fields on a Redis OM model using Python _type annotations_. If you
aren't familiar with type annotations, check out this
[tutorial](https://towardsdatascience.com/type-annotations-in-python-d90990b172dc).

This works exactly the same way as it does with Pydantic. Check out the [Pydantic documentation on field types](https://pydantic-docs.helpmanual.io/usage/types/) for guidance.

### With HashModel

`HashModel` stores data in Redis Hashes, which are flat. This means that a Redis Hash can't contain a Redis Set, List, or Hash. Because of this requirement, `HashModel` also does not currently support container types, such as:

* Sets
* Lists
* Dictionaries and other "mapping" types
* Other Redis OM models
* Pydantic models

**NOTE**: In the future, we may serialize these values as JSON strings, the same way we do for `JsonModel`. The difference would be that in the case of `HashModel`, you wouldn't be able to index these fields, just get and save them with the model. With `JsonModel`, you can index list fields and embedded `JsonModel`s.

So, in short, if you want to use container types, use `JsonModel`.

### With JsonModel

Good news! Container types _are_ supported with `JsonModel`.

We will use Pydantic's JSON serialization and encoding to serialize your `JsonModel` and save it in Redis.

### Default Values

Fields can have default values. You set them by assigning a value to a field.

```python
import datetime
from typing import Optional

from redis_om import HashModel


class Customer(HashModel):
    first_name: str
    last_name: str
    email: str
    join_date: datetime.date
    age: int
    bio: Optional[str] = "Super dope"  # <- We added a default here
```

Now, if we create a `Customer` object without a `bio` field, it will use the default value.

```python
import datetime
from typing import Optional

from redis_om import HashModel


class Customer(HashModel):
    first_name: str
    last_name: str
    email: str
    join_date: datetime.date
    age: int
    bio: Optional[str] = "Super dope"


andrew = Customer(
    first_name="Andrew",
    last_name="Brookins",
    email="andrew.brookins@example.com",
    join_date=datetime.date.today(),
    age=38)  # <- Notice, we didn't give a bio!

print(andrew.bio)  # <- So we got the default value.
# > 'Super Dope'
```

The model will then save this default value to Redis the next time you call `save()`.

## Retrieving Sub-Values of a JsonModel

`JsonModel` stores each model as a JSON document in Redis. When you only need a
single field (or a nested field) you can retrieve just that sub-value with
`get_value()`, instead of loading and deserializing the whole document. This
maps directly to the Redis JSON [`JSON.GET key <path>`](https://redis.io/docs/latest/develop/data-types/json/)
command and is more efficient for large documents.

```python
import datetime
from typing import List, Optional

from redis_om import EmbeddedJsonModel, JsonModel


class Order(EmbeddedJsonModel):
    name: str
    created_on: datetime.datetime


class Address(EmbeddedJsonModel):
    city: str
    postal_code: str


class Customer(JsonModel):
    first_name: str
    join_date: datetime.date
    address: Address
    orders: Optional[List[Order]] = None


customer = Customer(
    first_name="Andrew",
    join_date=datetime.date.today(),
    address=Address(city="Portland", postal_code="11111"),
    orders=[
        Order(name="Coffee", created_on=datetime.datetime(2022, 1, 1)),
        Order(name="Tea", created_on=datetime.datetime(2022, 2, 1)),
    ],
)
await customer.save()

# Retrieve a single nested field without loading the whole document.
await Customer.get_value(customer.pk, "address__city")
# > 'Portland'

# Types are converted just like a full `get()` — here, back to a `date`.
await Customer.get_value(customer.pk, "join_date")
# > datetime.date(...)

# Paths that descend into a list return a list of every match.
await Customer.get_value(customer.pk, "orders__name")
# > ['Coffee', 'Tea']
```

`get_value()` accepts either a nested field path using the `__` separator (the
same syntax used by `update()`), or a raw JSONPath string starting with `$`. It
raises `NotFoundError` if no document exists for the given primary key. Pass
`raw=True` to receive the value(s) exactly as Redis returns them, skipping type
conversion.

> NOTE: `get_value()` is available on `JsonModel` only, because `HashModel`
> stores flat Redis hashes rather than JSON documents.

## Marking a Field as Indexed

If you're using the RediSearch module in your Redis instance, you can mark a field as "indexed." As soon as you mark any field in a model as indexed, Redis OM will automatically create and manage an secondary index for the model for you, allowing you to query on any indexed field.

To mark a field as indexed, you need to use the Redis OM `Field()` helper, like this:

```python
from redis_om import (
    Field,
    HashModel,
)


class Customer(HashModel):
    first_name: str
    last_name: str = Field(index=True)
```

In this example, we marked `Customer.last_name` as indexed.

### Indexing Every Field on a Model

The class-level `index=True` flag marks a model as indexed (so that
migrations create a RediSearch index for it), but it does **not** by itself
turn every field into an indexed field. You still need to opt each field
in, either with `Field(index=True)` or by setting a field-level option that
implies indexing (vector, full-text search, or sortable):

```python
from redis_om import HashModel, Field


class Customer(HashModel, index=True):
    first_name: str = Field(index=True)
    last_name: str = Field(index=True)
    email: str = Field(index=True)
    age: int = Field(index=True, sortable=True)
    bio: str = Field(full_text_search=True)
```

A field is included in the index when any of the following are true:

* It is marked `Field(index=True)`
* It has `Field(..., vector_options=...)` (vector fields are always indexed)
* It has `Field(..., full_text_search=True)`
* It has `Field(..., sortable=True)`

If you set `Field(index=False)` on a field, that field is excluded from the
index even when its model class is `index=True`.

### Running Migrations

After defining an indexed model, you must run migrations so that RediSearch
creates the index:

```python
from redis_om import Migrator
Migrator().run()
```

Or from the command line (after installing redis-om):

```bash
migrate
# or, to point at a specific module that contains your models
migrate --module myapp.models
# or, to see what migrations *would* run without applying them
migrate --dry-run
```

The CLI auto-detects indexed models in the given module and prints the
migrations it would apply, then prompts for confirmation before running them.

Migrations are idempotent: re-running them is a no-op unless the schema
changed, in which case the existing index is dropped (your data is preserved)
and rebuilt. The full migration reference lives in the project README.

## Vector Fields for Similarity Search

Redis OM supports vector fields for similarity search, enabling AI/ML use cases
like semantic search, recommendation systems, and retrieval-augmented generation
(RAG). Vector fields are backed by [RediSearch vector indexes][redis-vss-url]
and work with both `JsonModel` and `HashModel`.

### Defining a Vector Field

Use `Field(..., vector_options=...)` with a `VectorFieldOptions` configuration to
declare a vector field. The Python type must be a numeric container, typically
`list[float]`:

```python
from redis_om import JsonModel, Field, VectorFieldOptions


class Document(JsonModel, index=True):
    title: str = Field(index=True)
    content: str = Field(full_text_search=True)
    embedding: list[float] = Field(
        vector_options=VectorFieldOptions.flat(
            type=VectorFieldOptions.TYPE.FLOAT32,
            dimension=384,  # Must match your embedding model's output size
            distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
        )
    )
```

A vector field is automatically added to the RediSearch index, even if you do
not pass `index=True` explicitly on the field — `vector_options` implies
indexing.

### Choosing an Algorithm: FLAT vs HNSW

`VectorFieldOptions` offers two algorithms:

**FLAT** — brute-force exact search. Best for smaller datasets (up to ~10K
vectors) or when recall must be 100%:

```python
vector_options = VectorFieldOptions.flat(
    type=VectorFieldOptions.TYPE.FLOAT32,
    dimension=768,
    distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
    initial_cap=1000,   # Optional: pre-allocate capacity
    block_size=1000,    # Optional: tune for batch insertions
)
```

**HNSW** — approximate nearest neighbor (ANN) search. Best for larger datasets
where sub-linear query latency matters:

```python
vector_options = VectorFieldOptions.hnsw(
    type=VectorFieldOptions.TYPE.FLOAT32,
    dimension=768,
    distance_metric=VectorFieldOptions.DISTANCE_METRIC.COSINE,
    m=16,                  # Optional: max outgoing edges per node
    ef_construction=200,   # Optional: index-time search width
    ef_runtime=10,         # Optional: query-time search width
    epsilon=0.01,          # Optional: distance threshold for HNSW
)
```

### Distance Metrics

Choose the metric that matches how your embedding model was trained:

| Metric    | Best for                                                         |
| --------- | ---------------------------------------------------------------- |
| `COSINE`  | Normalized text embeddings (most common)                         |
| `L2`      | Euclidean distance — image embeddings, some vision models        |
| `IP`      | Inner product — when vectors are pre-normalized for dot product  |

### Vector Data Types

- `FLOAT32` — 32-bit floats (most common, default for most models)
- `FLOAT64` — 64-bit floats (double precision)

### Nested Vector Fields (JsonModel only)

`JsonModel` supports vector fields nested inside a list — useful for storing
multiple embeddings per record (e.g., one per chunk of a long document):

```python
from typing import List

class ChunkedDocument(JsonModel, index=True):
    title: str = Field(index=True)
    # A list of embeddings, one per chunk
    chunk_embeddings: List[List[float]] = Field(
        vector_options=vector_options
    )
```

### Verifying Your Schema

After running migrations, you can inspect the generated RediSearch schema:

```python
print(Document.redisearch_schema())
# ON JSON PREFIX 1 your-app:Document:index SCHEMA
#   $.title AS title TAG SEPARATOR |
#   $.content AS content TAG SEPARATOR | $.content AS content_fts TEXT
#   $.embedding AS embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 384 DISTANCE_METRIC COSINE
```

For querying vector fields, see [Vector Similarity Search](getting_started.md#vector-similarity-search) in the Getting Started guide.

To create the indexes for any models that have indexed fields, use the `migrate` CLI command that Redis OM installs in your Python environment.

This command detects any `JsonModel` or `HashModel` instances in your project and does the following for each model that isn't abstract or embedded:

* If no index exists yet for the model:
  * The migrator creates an index
  * The migrator stores a hash of the index definition
* If an index exists for the model:
  * The migrator checks if the stored hash for the index is out of date
  * If the stored hash is out of date, the migrator drops the index (not your data!) and rebuilds it with the new index definition

You can also run the `Migrator` yourself with code:

```python
from redis_om import (
    get_redis_connection,
    Migrator
)

redis = get_redis_connection()
Migrator().run()
```

<!-- Links -->

[redis-vss-url]: https://redis.io/docs/latest/develop/interact/search-and-query/vectors/
