from dataclasses import dataclass
from typing import List, Optional

from . import barq_pb2


def _enum_suffix(name: str, prefix: str) -> str:
    if not name.startswith(prefix):
        raise ValueError(f"unexpected enum value {name!r}")
    return name[len(prefix) :].lower()


def _enum_member(name: str, prefix: str, allowed: set) -> str:
    suffix = _enum_suffix(name, prefix)
    if suffix not in allowed:
        raise ValueError(f"unsupported enum value {name!r}")
    return suffix


@dataclass(frozen=True)
class MetricDefinition:
    name: str
    kind: str
    description: str
    unit: Optional[str]
    labels: List[str]


@dataclass(frozen=True)
class TenantMemorySample:
    tenant: str
    resident_vector_memory_bytes: int


@dataclass(frozen=True)
class CollectionMemorySample:
    tenant: str
    collection: str
    resident_vector_memory_bytes: int


@dataclass(frozen=True)
class CollectionWalSample:
    tenant: str
    collection: str
    entries: int
    bytes: int


@dataclass(frozen=True)
class CollectionSegmentFileSample:
    tenant: str
    collection: str
    state: str
    count: int


@dataclass(frozen=True)
class CollectionSegmentStateSample:
    tenant: str
    collection: str
    state: str
    active: bool


@dataclass(frozen=True)
class StorageMetrics:
    refresh_count: int
    total_resident_vector_memory_bytes: int
    wal_appends_total: int
    wal_bytes_written_total: int
    compactions_total: int
    tenant_memory_bytes: List[TenantMemorySample]
    collection_memory_bytes: List[CollectionMemorySample]
    collection_wal: List[CollectionWalSample]
    collection_segment_files: List[CollectionSegmentFileSample]
    collection_segment_states: List[CollectionSegmentStateSample]


@dataclass(frozen=True)
class Metrics:
    definitions: List[MetricDefinition]
    storage: StorageMetrics


@dataclass(frozen=True)
class ClusterStatus:
    node_id: str
    mode: str
    write_durability: str
    shard_count: int
    node_count: int


@dataclass(frozen=True)
class SegmentCount:
    state: str
    count: int


@dataclass(frozen=True)
class CollectionSegmentInfo:
    tenant: str
    collection: str
    current_state: str
    index_state: str
    segment_counts: List[SegmentCount]


@dataclass(frozen=True)
class SegmentInfo:
    collections: List[CollectionSegmentInfo]


def metrics_from_proto(response) -> Metrics:
    if not response.HasField("storage"):
        raise ValueError("metrics response missing storage payload")

    return Metrics(
        definitions=[
            MetricDefinition(
                name=definition.name,
                kind=_enum_member(
                    barq_pb2.MetricKind.Name(definition.kind),
                    "METRIC_KIND_",
                    {"counter", "gauge", "histogram"},
                ),
                description=definition.description,
                unit=definition.unit or None,
                labels=list(definition.labels),
            )
            for definition in response.definitions
        ],
        storage=StorageMetrics(
            refresh_count=response.storage.refresh_count,
            total_resident_vector_memory_bytes=response.storage.total_resident_vector_memory_bytes,
            wal_appends_total=response.storage.wal_appends_total,
            wal_bytes_written_total=response.storage.wal_bytes_written_total,
            compactions_total=response.storage.compactions_total,
            tenant_memory_bytes=[
                TenantMemorySample(
                    tenant=sample.tenant,
                    resident_vector_memory_bytes=sample.resident_vector_memory_bytes,
                )
                for sample in response.storage.tenant_memory_bytes
            ],
            collection_memory_bytes=[
                CollectionMemorySample(
                    tenant=sample.tenant,
                    collection=sample.collection,
                    resident_vector_memory_bytes=sample.resident_vector_memory_bytes,
                )
                for sample in response.storage.collection_memory_bytes
            ],
            collection_wal=[
                CollectionWalSample(
                    tenant=sample.tenant,
                    collection=sample.collection,
                    entries=sample.entries,
                    bytes=sample.bytes,
                )
                for sample in response.storage.collection_wal
            ],
            collection_segment_files=[
                CollectionSegmentFileSample(
                    tenant=sample.tenant,
                    collection=sample.collection,
                    state=_enum_member(
                        barq_pb2.SegmentState.Name(sample.state),
                        "SEGMENT_STATE_",
                        {"growing", "sealed", "compacted"},
                    ),
                    count=sample.count,
                )
                for sample in response.storage.collection_segment_files
            ],
            collection_segment_states=[
                CollectionSegmentStateSample(
                    tenant=sample.tenant,
                    collection=sample.collection,
                    state=_enum_member(
                        barq_pb2.SegmentState.Name(sample.state),
                        "SEGMENT_STATE_",
                        {"growing", "sealed", "compacted"},
                    ),
                    active=sample.active,
                )
                for sample in response.storage.collection_segment_states
            ],
        ),
    )


def cluster_status_from_proto(response) -> ClusterStatus:
    return ClusterStatus(
        node_id=response.node_id,
        mode=_enum_member(
            barq_pb2.ClusterMode.Name(response.mode),
            "CLUSTER_MODE_",
            {"single_node", "routed_replication", "consensus_backed"},
        ),
        write_durability=_enum_member(
            barq_pb2.WriteDurability.Name(response.write_durability),
            "WRITE_DURABILITY_",
            {"node_local", "primary_only", "consensus_quorum"},
        ),
        shard_count=response.shard_count,
        node_count=response.node_count,
    )


def segment_info_from_proto(response) -> SegmentInfo:
    return SegmentInfo(
        collections=[
            CollectionSegmentInfo(
                tenant=collection.tenant,
                collection=collection.collection,
                current_state=_enum_member(
                    barq_pb2.SegmentState.Name(collection.current_state),
                    "SEGMENT_STATE_",
                    {"growing", "sealed", "compacted"},
                ),
                index_state=_enum_member(
                    barq_pb2.IndexState.Name(collection.index_state),
                    "INDEX_STATE_",
                    {"building", "ready", "stale"},
                ),
                segment_counts=[
                    SegmentCount(
                        state=_enum_member(
                            barq_pb2.SegmentState.Name(count.state),
                            "SEGMENT_STATE_",
                            {"growing", "sealed", "compacted"},
                        ),
                        count=count.count,
                    )
                    for count in collection.segment_counts
                ],
            )
            for collection in response.collections
        ]
    )
