import type * as grpc from '@grpc/grpc-js';
import type { MessageTypeDefinition } from '@grpc/proto-loader';

import type { BarqClient as _barq_BarqClient, BarqDefinition as _barq_BarqDefinition } from './barq/Barq';

type SubtypeConstructor<Constructor extends new (...args: any) => any, Subtype> = {
  new(...args: ConstructorParameters<Constructor>): Subtype;
};

export interface ProtoGrpcType {
  barq: {
    Barq: SubtypeConstructor<typeof grpc.Client, _barq_BarqClient> & { service: _barq_BarqDefinition }
    BatchSearchRequest: MessageTypeDefinition
    BatchSearchResponse: MessageTypeDefinition
    CreateCollectionRequest: MessageTypeDefinition
    CreateCollectionResponse: MessageTypeDefinition
    HealthRequest: MessageTypeDefinition
    HealthResponse: MessageTypeDefinition
    InsertDocumentRequest: MessageTypeDefinition
    InsertDocumentResponse: MessageTypeDefinition
    InsertRequest: MessageTypeDefinition
    InsertResponse: MessageTypeDefinition
    QueryResults: MessageTypeDefinition
    SearchQuery: MessageTypeDefinition
    SearchRequest: MessageTypeDefinition
    SearchResponse: MessageTypeDefinition
    SearchResult: MessageTypeDefinition
    StatusRequest: MessageTypeDefinition
    StatusResponse: MessageTypeDefinition
  }
}

