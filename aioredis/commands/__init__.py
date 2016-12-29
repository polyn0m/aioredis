import asyncio

from aioredis.connection import create_connection
from aioredis.pool import create_pool
from aioredis.util import _NOTSET
from .generic import GenericCommandsMixin
from .string import StringCommandsMixin
from .hash import HashCommandsMixin
from .hyperloglog import HyperLogLogCommandsMixin
from .set import SetCommandsMixin
from .sorted_set import SortedSetCommandsMixin
from .transaction import TransactionsCommandsMixin, Pipeline, MultiExec
from .list import ListCommandsMixin
from .scripting import ScriptingCommandsMixin
from .server import ServerCommandsMixin
from .pubsub import PubSubCommandsMixin
from .cluster import ClusterCommandsMixin

__all__ = ['create_redis', 'create_redis_pool',
           'Redis', 'Pipeline', 'MultiExec']


class Redis(GenericCommandsMixin, StringCommandsMixin,
            HyperLogLogCommandsMixin, SetCommandsMixin,
            HashCommandsMixin, TransactionsCommandsMixin,
            SortedSetCommandsMixin, ListCommandsMixin,
            ScriptingCommandsMixin, ServerCommandsMixin,
            PubSubCommandsMixin, ClusterCommandsMixin,
            ):
    """High-level Redis interface.

    Gathers in one place Redis commands implemented in mixins.

    For commands details see: http://redis.io/commands/#connection
    """
    def __init__(self, pool_or_conn):
        self._pool_or_conn = pool_or_conn

    def __repr__(self):
        return '<Redis {!r}>'.format(self._pool_or_conn)

    def execute(self, command, *args, **kwargs):
        return self._pool_or_conn.execute(command, *args, **kwargs)

    def close(self):
        self._pool_or_conn.close()

    @asyncio.coroutine
    def wait_closed(self):
        yield from self._pool_or_conn.wait_closed()

    @property
    def db(self):
        """Currently selected db index."""
        return self._pool_or_conn.db

    @property
    def encoding(self):
        """Current set codec or None."""
        return self._pool_or_conn.encoding

    @property
    def connection(self):
        """Either :class:`aioredis.RedisConnection`,
        or :class:`aioredis.ConnectionsPool` instance.
        """
        return self._pool_or_conn

    @property
    def in_transaction(self):
        """Set to True when MULTI command was issued."""
        # XXX: this must be bound to real connection
        return self._pool_or_conn.in_transaction

    @property
    def closed(self):
        """True if connection is closed."""
        return self._pool_or_conn.closed

    def auth(self, password):
        """Authenticate to server.

        This method wraps call to :meth:`aioredis.RedisConnection.auth()`
        """
        return self._pool_or_conn.auth(password)

    def echo(self, message, *, encoding=_NOTSET):
        """Echo the given string."""
        return self.execute('ECHO', message, encoding=encoding)

    def ping(self, *, encoding=_NOTSET):
        """Ping the server."""
        return self.execute('PING', encoding=encoding)

    def quit(self):
        """Close the connection."""
        # TODO: warn when using pool
        return self.execute('QUIT')

    def select(self, db):
        """Change the selected database for the current connection.

        This method wraps call to :meth:`aioredis.RedisConnection.select()`
        """
        return self._pool_or_conn.select(db)


@asyncio.coroutine
def create_redis(address, *, db=None, password=None, ssl=None,
                 encoding=None, commands_factory=Redis,
                 loop=None):
    """Creates high-level Redis interface.

    This function is a coroutine.
    """
    conn = yield from create_connection(address, db=db,
                                        password=password,
                                        ssl=ssl,
                                        encoding=encoding,
                                        loop=loop)
    return commands_factory(conn)


@asyncio.coroutine
def create_redis_pool(address, *, db=None, password=None, ssl=None,
                      encoding=None, commands_factory=Redis,
                      minsize=1, maxsize=10,
                      loop=None):
    """Creates high-level Redis interface.

    This function is a coroutine.
    """
    pool = yield from create_pool(address, db=db,
                                  password=password,
                                  ssl=ssl,
                                  encoding=encoding,
                                  minsize=minsize,
                                  maxsize=maxsize,
                                  loop=loop)
    return commands_factory(pool)


create_reconnecting_redis = create_redis_pool
# @asyncio.coroutine
# def create_reconnecting_redis(address, *, db=None, password=None, ssl=None,
#                               encoding=None, commands_factory=Redis,
#                               loop=None):
#     """Creates high-level Redis interface.
#
#     This function is a coroutine.
#     """
#     # Note: this is not coroutine, but we may make it such. We may start
#     # a first connection in it, or just resolve DNS. So let's keep it
#     # coroutine for forward compatibility
#     conn = AutoConnector(address,
#                          db=db, password=password, ssl=ssl,
#                          encoding=encoding, loop=loop)
#     return commands_factory(conn)


# make pyflakes happy
(Pipeline, MultiExec)
