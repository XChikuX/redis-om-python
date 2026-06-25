# Connecting to Redis

You can control how Redis OM connects to Redis with the `REDIS_OM_URL` environment variable, or by manually constructing Redis client objects.

## Environment Variable

By default, Redis OM tries to connect to Redis on your localhost at port 6379. Most local install methods will result in Redis running at this location, in which case you don't need to do anything special for Redis OM to connect to Redis.

However, if you configured Redis to run on a different port, or if you're using a remote Redis server, you'll need to set the `REDIS_OM_URL` environment variable.

The `REDIS_OM_URL` environment variable follows the redis-py URL format:

    redis://[[username]:[password]]@localhost:6379/[database number]

**NOTE:** The square brackets indicate an optional value and are not part of the URL format.

The default connection is equivalent to the following `REDIS_OM_URL` environment variable:

    redis://localhost:6379

**Note:** Indexing only works for data stored in Redis logical database 0.  If you are using a different database number when connecting to Redis, you can expect the code to raise a `MigrationError` when you run the migrator.

### Passwords and Usernames

Redis can be configured with password protection and a "default" user, in which case you might connect using only the password.

You can do so with Redis OM like this:

    redis://:your-password@localhost:6379

If your Redis instance requires both a username and a password, you would include both in the URL:

    redis://your-username:your-password@localhost:6379

### Database Number

Redis databases are numbered, and the default is 0. You can leave off the database number to use the default database, or specify it.

**Note:** Indexing only works for data stored in Redis logical database 0.  If you are using a different database number when connecting to Redis, you can expect the code to raise a `MigrationError` when you run the migrator.

### SSL Connections

Use the "rediss" prefix for SSL connections:

    rediss://[[username]:[password]]@localhost:6379/0

### Unix Domain Sockets

Use the "unix" prefix to connect to Redis over Unix domain sockets:

    unix://[[username]:[password]]@/path/to/socket.sock?db=0

### To Learn More

To learn more about the URL format that Redis OM Python uses, consult the [redis-py URL documentation](https://redis-py.readthedocs.io/en/stable/#redis.Redis.from_url).

**TIP:** The URL format is the same if you're using async or sync mode with Redis OM (i.e., importing `aredis_om` for async or `redis_om` for sync).

## Connection Objects

Aside from controlling connections via the `REDIS_OM_URL` environment variable, you can manually construct Redis client connections for a specific OM model class.

**NOTE:** This method takes precedence over the `REDIS_OM_URL` environment variable.

You can control the connection a specific model class should use by assigning an object to the *database* field of a model's _Meta_ object, like so:

```python
from redis_om import HashModel, get_redis_connection


redis = get_redis_connection(port=6378)


class Customer(HashModel):
    first_name: str
    last_name: str
    age: int

    class Meta:
        database = redis
```

The `get_redis_connection()` function is a Redis OM helper that passes keyword arguments to either `redis.asyncio.Redis.from_url()` or `redis.Redis.from_url()`, depending on whether you are using Redis OM in async or sync mode.

You can also manually construct a client object:

```python
from redis import Redis

from redis_om import HashModel


class Customer(HashModel):
    first_name: str
    last_name: str
    age: int

    class Meta:
        database = Redis(port=6378)
```

## Redis Cluster

Redis OM also supports connecting to a Redis Cluster. Either pass
`cluster=True` to `get_redis_connection()` or add `cluster=true` as a query
parameter to your `REDIS_OM_URL`:

```python
from redis_om import HashModel, get_redis_connection


# Via keyword argument
redis = get_redis_connection(cluster=True, host="node1", port=6379)


# Or via the URL environment variable
# REDIS_OM_URL=redis://node1:6379,node2:6379,node3:6379/?cluster=true
class Customer(HashModel):
    first_name: str
    last_name: str

    class Meta:
        database = redis
```

The `cluster=true` query parameter is consumed by `get_redis_connection()`
and stripped before the URL is forwarded to `redis.RedisCluster` so it does
not interfere with cluster initialization.

## RESP2 vs RESP3

Redis OM works against either RESP2 or RESP3 wire protocols.  redis-py 8.0+
auto-negotiates the protocol with the server on connect, so most users will
not need to think about it: Redis 6+ defaults to RESP3 and older servers
fall back to RESP2 transparently.

If you need to pin the protocol explicitly (for example to reproduce a
behaviour seen in production), use the `protocol=` URL query parameter or the
matching keyword argument to `get_redis_connection()`:

```python
from redis_om import get_redis_connection

# Pin to RESP3 via the URL
redis = get_redis_connection(
    url="redis://localhost:6379?decode_responses=True&protocol=3"
)

# Or pin via a keyword argument
redis = get_redis_connection(
    url="redis://localhost:6379?decode_responses=True",
    protocol=2,
)
```

You can also read the negotiated protocol version from any active client:

```python
from redis_om import get_redis_connection, protocol_version

redis = get_redis_connection()
print(protocol_version(redis))  # 2 or 3
```

The library normalises both protocol shapes internally for `FT.SEARCH`,
`FT.AGGREGATE`, and `FT.AGGREGATE WITHCURSOR` so your application code does
not need to branch on the protocol.
