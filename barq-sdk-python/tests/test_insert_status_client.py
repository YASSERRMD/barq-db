import unittest

from barq import GrpcClient
from barq.barq_pb2 import (
    GetInsertStatusResponse,
    INSERT_STATUS_STATE_PROCESSING,
)


class _FakeStatusStub:
    def GetInsertStatus(self, request, metadata=None):
        return GetInsertStatusResponse(
            request_id=request.request_id,
            state=INSERT_STATUS_STATE_PROCESSING,
            error_message="",
        )


class InsertStatusClientTest(unittest.TestCase):
    def test_grpc_client_maps_insert_status_response(self) -> None:
        client = GrpcClient("127.0.0.1:50051")
        client.stub = _FakeStatusStub()

        status = client.get_insert_status("ingest-22")

        self.assertEqual(
            status,
            {
                "request_id": "ingest-22",
                "state": "processing",
                "error_message": None,
            },
        )
        client.close()


if __name__ == "__main__":
    unittest.main()
