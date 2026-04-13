import type * as grpc from '@grpc/grpc-js';
import type { EnumTypeDefinition, MessageTypeDefinition } from '@grpc/proto-loader';

import type { BarqClient as _barq_BarqClient, BarqDefinition as _barq_BarqDefinition } from './barq/Barq';

type SubtypeConstructor<Constructor extends new (...args: any) => any, Subtype> = {
  new(...args: ConstructorParameters<Constructor>): Subtype;
};

export interface ProtoGrpcType {
  barq: {
    Barq: SubtypeConstructor<typeof grpc.Client, _barq_BarqClient> & { service: _barq_BarqDefinition }
    BatchSearchRequest: MessageTypeDefinition
    BatchSearchResponse: MessageTypeDefinition
    ClusterMode: EnumTypeDefinition
    CollectionMemorySample: MessageTypeDefinition
    CollectionSegmentFileSample: MessageTypeDefinition
    CollectionSegmentInfo: MessageTypeDefinition
    CollectionSegmentStateSample: MessageTypeDefinition
    CollectionWalSample: MessageTypeDefinition
    Consistency: EnumTypeDefinition
    CreateCollectionRequest: MessageTypeDefinition
    CreateCollectionResponse: MessageTypeDefinition
    GetClusterStatusRequest: MessageTypeDefinition
    GetClusterStatusResponse: MessageTypeDefinition
    GetInsertStatusRequest: MessageTypeDefinition
    GetInsertStatusResponse: MessageTypeDefinition
    GetMetricsRequest: MessageTypeDefinition
    GetMetricsResponse: MessageTypeDefinition
    GetSegmentInfoRequest: MessageTypeDefinition
    GetSegmentInfoResponse: MessageTypeDefinition
    HealthRequest: MessageTypeDefinition
    HealthResponse: MessageTypeDefinition
    IndexState: EnumTypeDefinition
    InsertAsyncResponse: MessageTypeDefinition
    InsertDocumentRequest: MessageTypeDefinition
    InsertDocumentResponse: MessageTypeDefinition
    InsertOptions: MessageTypeDefinition
    InsertRequest: MessageTypeDefinition
    InsertResponse: MessageTypeDefinition
    InsertStatusState: EnumTypeDefinition
    MetricDefinition: MessageTypeDefinition
    MetricKind: EnumTypeDefinition
    QueryResults: MessageTypeDefinition
    SearchOptions: MessageTypeDefinition
    SearchQuery: MessageTypeDefinition
    SearchRequest: MessageTypeDefinition
    SearchResponse: MessageTypeDefinition
    SearchResult: MessageTypeDefinition
    SegmentCount: MessageTypeDefinition
    SegmentState: EnumTypeDefinition
    StatusRequest: MessageTypeDefinition
    StatusResponse: MessageTypeDefinition
    StorageMetrics: MessageTypeDefinition
    TenantMemorySample: MessageTypeDefinition
    WriteDurability: EnumTypeDefinition
  }
}

