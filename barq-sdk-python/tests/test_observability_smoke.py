import os
import unittest

from barq import BarqClient
from barq.barq_pb2 import CLUSTER_MODE_SINGLE_NODE


class ObservabilitySmokeTest(unittest.TestCase):
    def test_public_client_reads_metrics_and_cluster_status(self) -> None:
        client = BarqClient(
            os.environ.get("BARQ_BASE_URL", "http://127.0.0.1:8080"),
            os.environ.get("BARQ_API_KEY", ""),
        )

        metrics = client.get_metrics()
        self.assertTrue(metrics.definitions)
        self.assertGreater(metrics.storage.total_resident_vector_memory_bytes, 0)

        status = client.get_cluster_status()
        self.assertEqual(status.mode, CLUSTER_MODE_SINGLE_NODE)
        self.assertEqual(status.node_count, 1)
        self.assertEqual(status.shard_count, 1)
        client.close()


if __name__ == "__main__":
    unittest.main()
