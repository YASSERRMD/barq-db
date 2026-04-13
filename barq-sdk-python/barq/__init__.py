from .grpc_client import GrpcClient
from .observability import ClusterStatus, CollectionSegmentInfo, Metrics, SegmentInfo

__all__ = [
    "BarqClient",
    "GrpcClient",
    "Metrics",
    "ClusterStatus",
    "SegmentInfo",
    "CollectionSegmentInfo",
]


def __getattr__(name):
    if name == "BarqClient":
        from .client import BarqClient

        return BarqClient
    if name == "GrpcClient":
        return GrpcClient
    if name == "Metrics":
        return Metrics
    if name == "ClusterStatus":
        return ClusterStatus
    if name == "SegmentInfo":
        return SegmentInfo
    if name == "CollectionSegmentInfo":
        return CollectionSegmentInfo
    raise AttributeError(f"module 'barq' has no attribute {name!r}")
