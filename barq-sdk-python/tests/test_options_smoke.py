import os
import unittest

from barq import BarqClient


class OptionsSmokeTest(unittest.TestCase):
    def test_public_client_insert_and_search_with_options(self) -> None:
        client = BarqClient(
            os.environ.get("BARQ_BASE_URL", "http://127.0.0.1:8080"),
            os.environ.get("BARQ_API_KEY", ""),
        )
        collection = os.environ["BARQ_TEST_COLLECTION"]

        self.assertTrue(client.health())
        client.create_collection(collection, 2, "Cosine")
        client.insert_document(
            collection,
            "python-options-doc",
            [1.0, 0.0],
            {"sdk": "python", "mode": "options"},
            options={"wait_for_commit": True},
        )

        results = client.search(
            collection,
            [1.0, 0.0],
            top_k=1,
            options={"consistency": "primary", "allow_fallback": True},
        )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], {"Str": "python-options-doc"})

        client.close()


if __name__ == "__main__":
    unittest.main()
