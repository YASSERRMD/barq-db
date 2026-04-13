import json
import os
import unittest

from barq import BarqClient


class GoldenClientTest(unittest.TestCase):
    def test_public_client_matches_rest_baseline(self) -> None:
        client = BarqClient(
            os.environ.get("BARQ_BASE_URL", "http://127.0.0.1:8080"),
            os.environ["BARQ_API_KEY"],
        )
        collection = os.environ.get("BARQ_TEST_COLLECTION", "sdk-python-golden")
        expected = json.loads(os.environ["BARQ_EXPECTED_RESULTS"])

        self.assertTrue(client.health())
        client.create_collection(collection, 2, "Cosine")
        client.insert_document(
            collection,
            "golden-primary",
            [1.0, 0.0],
            {"sdk": "python", "mode": "golden"},
        )
        client.insert_document(
            collection,
            "golden-secondary",
            [0.0, 1.0],
            {"sdk": "python", "mode": "golden"},
        )

        results = client.search(collection, [1.0, 0.0], top_k=2)
        self.assertEqual(results, expected)

        client.close()


if __name__ == "__main__":
    unittest.main()
