"""Persistence tests for corrected signal invalidation."""
import unittest

from app.store import Store


class FakeConnection:
    def __init__(self, error=None):
        self.error = error
        self.query = None
        self.args = None

    async def execute(self, query, *args):
        self.query = query
        self.args = args
        if self.error:
            raise self.error


class FakeAcquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *_):
        return False


class FakePool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return FakeAcquire(self.connection)


class StoreInvalidationTests(unittest.IsolatedAsyncioTestCase):
    async def test_invalidation_marks_payload_unconfirmed_and_retains_row(self):
        connection = FakeConnection()
        store = Store()
        store.pool = FakePool(connection)

        self.assertTrue(await store.invalidate("BTCUSDT", "5m", 12345))
        self.assertIn("UPDATE signals SET payload=jsonb_set", connection.query)
        self.assertIn("'{confirmed}','false'::jsonb", connection.query)
        self.assertIn("'{invalidated}','true'::jsonb", connection.query)
        self.assertIn("WHERE symbol=$1 AND timeframe=$2 AND candle_ms=$3", connection.query)
        self.assertEqual(connection.args, ("BTCUSDT", "5m", 12345))

    async def test_invalidation_database_error_returns_false(self):
        store = Store()
        store.pool = FakePool(FakeConnection(RuntimeError("database unavailable")))

        self.assertFalse(await store.invalidate("BTCUSDT", "5m", 12345))


if __name__ == "__main__":
    unittest.main()
