import os
import unittest

from barq import GrpcClient


class GrpcSmokeTest(unittest.TestCase):
    def test_insert_and_search(self) -> None:
        target = os.environ["BARQ_GRPC_ADDR"]
        collection = os.environ["BARQ_TEST_COLLECTION"]

        client = GrpcClient(target)
        self.assertTrue(client.status())

        client.create_collection(collection, 2, "Cosine")
        client.insert(
            collection,
            "python-doc",
            [1.0, 0.0],
            {"sdk": "python", "mode": "grpc"},
        )
        results = client.search(collection, [1.0, 0.0], 1)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], "python-doc")


if __name__ == "__main__":
    unittest.main()
