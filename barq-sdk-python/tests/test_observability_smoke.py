import os
import unittest

from barq import BarqClient


class ObservabilitySmokeTest(unittest.TestCase):
    def test_public_client_reads_metrics_and_cluster_status(self) -> None:
        client = BarqClient(
            os.environ.get("BARQ_BASE_URL", "http://127.0.0.1:8080"),
            os.environ.get("BARQ_API_KEY", ""),
        )
        collection = os.environ["BARQ_TEST_COLLECTION"]

        metrics = client.get_metrics()
        self.assertTrue(metrics.definitions)
        self.assertGreater(metrics.storage.total_resident_vector_memory_bytes, 0)
        self.assertTrue(
            any(sample.collection == collection for sample in metrics.storage.collection_wal)
        )

        status = client.get_cluster_status()
        self.assertEqual(status.mode, "single_node")
        self.assertEqual(status.node_count, 1)
        self.assertEqual(status.shard_count, 1)

        segment_info = client.get_segment_info(collection)
        self.assertEqual(len(segment_info.collections), 1)
        self.assertEqual(segment_info.collections[0].collection, collection)
        client.close()


if __name__ == "__main__":
    unittest.main()
