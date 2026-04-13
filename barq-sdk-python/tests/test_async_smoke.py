import os
import unittest

from barq import BarqClient


class AsyncSmokeTest(unittest.IsolatedAsyncioTestCase):
    async def test_async_insert_returns_request_id(self) -> None:
        client = BarqClient(
            os.environ.get("BARQ_BASE_URL", "http://127.0.0.1:8080"),
            os.environ.get("BARQ_API_KEY", ""),
        )
        collection = os.environ["BARQ_TEST_COLLECTION"]

        client.create_collection(collection, 2, "Cosine")
        request_id = await client.insert_async(
            collection,
            "python-async-doc",
            [1.0, 0.0],
            {"sdk": "python", "mode": "async"},
        )

        self.assertTrue(request_id.startswith("ingest-"))
        client.close()


if __name__ == "__main__":
    unittest.main()
