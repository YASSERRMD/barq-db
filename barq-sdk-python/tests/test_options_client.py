import unittest

from barq import BarqClient, GrpcClient
from barq.barq_pb2 import (
    CONSISTENCY_FOLLOWERS,
    InsertResponse,
    SearchResponse,
    SearchResult,
)


class _FakeStub:
    def __init__(self) -> None:
        self.insert_request = None
        self.search_request = None

    def Insert(self, request, metadata=None):
        self.insert_request = request
        self.insert_metadata = metadata
        return InsertResponse(success=True)

    def Search(self, request, metadata=None):
        self.search_request = request
        self.search_metadata = metadata
        return SearchResponse(results=[SearchResult(id="doc-1", score=1.0, payload_json="{}")])


class GrpcOptionsClientTest(unittest.TestCase):
    def test_grpc_insert_encodes_wait_for_commit(self) -> None:
        client = GrpcClient("127.0.0.1:50051")
        fake_stub = _FakeStub()
        client.stub = fake_stub

        client.insert(
            "docs",
            "python-doc",
            [1.0, 0.0],
            {"sdk": "python"},
            options={"wait_for_commit": False},
        )

        self.assertFalse(fake_stub.insert_request.options.wait_for_commit)
        client.close()

    def test_grpc_search_encodes_consistency_and_default_fallback(self) -> None:
        client = GrpcClient("127.0.0.1:50051")
        fake_stub = _FakeStub()
        client.stub = fake_stub

        client.search(
            "docs",
            [1.0, 0.0],
            1,
            options={"consistency": "followers"},
        )

        self.assertEqual(
            fake_stub.search_request.options.consistency,
            CONSISTENCY_FOLLOWERS,
        )
        self.assertTrue(fake_stub.search_request.options.allow_fallback)
        client.close()

    def test_public_client_rejects_options_on_rest_fallback(self) -> None:
        client = BarqClient("http://127.0.0.1:8080", "test-key")

        with self.assertRaisesRegex(
            ValueError,
            "advanced search options are only supported for vector-only gRPC search",
        ):
            client.search(
                "docs",
                vector=[1.0, 0.0],
                query="hello",
                options={"allow_fallback": False},
            )

        client.close()


if __name__ == "__main__":
    unittest.main()
