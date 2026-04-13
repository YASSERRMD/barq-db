from .grpc_client import GrpcClient

__all__ = ["BarqClient", "GrpcClient"]


def __getattr__(name):
    if name == "BarqClient":
        from .client import BarqClient

        return BarqClient
    if name == "GrpcClient":
        return GrpcClient
    raise AttributeError(f"module 'barq' has no attribute {name!r}")
