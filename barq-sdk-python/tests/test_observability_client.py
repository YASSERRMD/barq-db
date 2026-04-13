import unittest

from barq import GrpcClient
from barq.barq_pb2 import (
    CLUSTER_MODE_SINGLE_NODE,
    GetClusterStatusResponse,
    GetMetricsResponse,
    GetSegmentInfoResponse,
    INDEX_STATE_READY,
    METRIC_KIND_GAUGE,
    MetricDefinition,
    SEGMENT_STATE_GROWING,
    SEGMENT_STATE_SEALED,
    StorageMetrics,
    TenantMemorySample,
    CollectionMemorySample,
    CollectionWalSample,
    CollectionSegmentFileSample,
    CollectionSegmentStateSample,
    CollectionSegmentInfo,
    SegmentCount,
)


class _FakeStub:
    def GetMetrics(self, request, metadata=None):
        return GetMetricsResponse(
            definitions=[
                MetricDefinition(
                    name="ingestion_queue_size",
                    kind=METRIC_KIND_GAUGE,
                    description="queue depth",
                    labels=["tenant"],
                )
            ],
            storage=StorageMetrics(
                refresh_count=1,
                total_resident_vector_memory_bytes=64,
                wal_appends_total=2,
                wal_bytes_written_total=32,
                compactions_total=0,
                tenant_memory_bytes=[
                    TenantMemorySample(
                        tenant="default",
                        resident_vector_memory_bytes=64,
                    )
                ],
                collection_memory_bytes=[
                    CollectionMemorySample(
                        tenant="default",
                        collection="docs",
                        resident_vector_memory_bytes=64,
                    )
                ],
                collection_wal=[
                    CollectionWalSample(
                        tenant="default",
                        collection="docs",
                        entries=2,
                        bytes=32,
                    )
                ],
                collection_segment_files=[
                    CollectionSegmentFileSample(
                        tenant="default",
                        collection="docs",
                        state=SEGMENT_STATE_SEALED,
                        count=1,
                    )
                ],
                collection_segment_states=[
                    CollectionSegmentStateSample(
                        tenant="default",
                        collection="docs",
                        state=SEGMENT_STATE_GROWING,
                        active=True,
                    )
                ],
            ),
        )

    def GetClusterStatus(self, request, metadata=None):
        return GetClusterStatusResponse(
            node_id="local",
            mode=CLUSTER_MODE_SINGLE_NODE,
            write_durability=1,
            shard_count=1,
            node_count=1,
        )

    def GetSegmentInfo(self, request, metadata=None):
        return GetSegmentInfoResponse(
            collections=[
                CollectionSegmentInfo(
                    tenant="default",
                    collection="docs",
                    current_state=SEGMENT_STATE_GROWING,
                    index_state=INDEX_STATE_READY,
                    segment_counts=[
                        SegmentCount(state=SEGMENT_STATE_SEALED, count=1)
                    ],
                )
            ]
        )


class GrpcObservabilityClientTest(unittest.TestCase):
    def test_grpc_metrics_map_to_structured_model(self) -> None:
        client = GrpcClient("127.0.0.1:50051")
        client.stub = _FakeStub()

        metrics = client.get_metrics()

        self.assertEqual(metrics.definitions[0].kind, "gauge")
        self.assertEqual(metrics.storage.collection_wal[0].collection, "docs")
        self.assertEqual(
            metrics.storage.collection_segment_states[0].state,
            "growing",
        )
        client.close()

    def test_grpc_cluster_status_maps_to_structured_model(self) -> None:
        client = GrpcClient("127.0.0.1:50051")
        client.stub = _FakeStub()

        status = client.get_cluster_status()

        self.assertEqual(status.mode, "single_node")
        self.assertEqual(status.write_durability, "node_local")
        self.assertEqual(status.node_count, 1)
        client.close()

    def test_grpc_cluster_status_rejects_unspecified_mode(self) -> None:
        client = GrpcClient("127.0.0.1:50051")
        fake_stub = _FakeStub()
        fake_stub.GetClusterStatus = lambda request, metadata=None: GetClusterStatusResponse(
            node_id="local",
            mode=0,
            write_durability=1,
            shard_count=1,
            node_count=1,
        )
        client.stub = fake_stub

        with self.assertRaisesRegex(ValueError, "unsupported enum value"):
            client.get_cluster_status()

        client.close()

    def test_grpc_segment_info_maps_to_structured_model(self) -> None:
        client = GrpcClient("127.0.0.1:50051")
        client.stub = _FakeStub()

        segment_info = client.get_segment_info("docs")

        self.assertEqual(segment_info.collections[0].collection, "docs")
        self.assertEqual(segment_info.collections[0].current_state, "growing")
        self.assertEqual(segment_info.collections[0].segment_counts[0].state, "sealed")
        client.close()


if __name__ == "__main__":
    unittest.main()
