import os
import unittest

from barq import BarqClient


class CompatClientTest(unittest.TestCase):
    def test_public_client_create_insert_search(self) -> None:
        client = BarqClient(
            os.environ.get("BARQ_BASE_URL", "http://127.0.0.1:8080"),
            os.environ["BARQ_API_KEY"],
        )

        self.assertTrue(client.health())
        client.create_collection("sdk-python-compat", 2, "Cosine")
        client.insert_document(
            "sdk-python-compat",
            "python-doc",
            [1.0, 0.0],
            {"sdk": "python", "mode": "compat"},
        )

        results = client.search("sdk-python-compat", [1.0, 0.0], top_k=1)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], {"Str": "python-doc"})

        client.close()


if __name__ == "__main__":
    unittest.main()
