// Original file: proto/barq.proto

import type * as grpc from '@grpc/grpc-js'
import type { MethodDefinition } from '@grpc/proto-loader'
import type { BatchSearchRequest as _barq_BatchSearchRequest, BatchSearchRequest__Output as _barq_BatchSearchRequest__Output } from '../barq/BatchSearchRequest';
import type { BatchSearchResponse as _barq_BatchSearchResponse, BatchSearchResponse__Output as _barq_BatchSearchResponse__Output } from '../barq/BatchSearchResponse';
import type { CreateCollectionRequest as _barq_CreateCollectionRequest, CreateCollectionRequest__Output as _barq_CreateCollectionRequest__Output } from '../barq/CreateCollectionRequest';
import type { CreateCollectionResponse as _barq_CreateCollectionResponse, CreateCollectionResponse__Output as _barq_CreateCollectionResponse__Output } from '../barq/CreateCollectionResponse';
import type { HealthRequest as _barq_HealthRequest, HealthRequest__Output as _barq_HealthRequest__Output } from '../barq/HealthRequest';
import type { HealthResponse as _barq_HealthResponse, HealthResponse__Output as _barq_HealthResponse__Output } from '../barq/HealthResponse';
import type { InsertAsyncResponse as _barq_InsertAsyncResponse, InsertAsyncResponse__Output as _barq_InsertAsyncResponse__Output } from '../barq/InsertAsyncResponse';
import type { InsertDocumentRequest as _barq_InsertDocumentRequest, InsertDocumentRequest__Output as _barq_InsertDocumentRequest__Output } from '../barq/InsertDocumentRequest';
import type { InsertDocumentResponse as _barq_InsertDocumentResponse, InsertDocumentResponse__Output as _barq_InsertDocumentResponse__Output } from '../barq/InsertDocumentResponse';
import type { InsertRequest as _barq_InsertRequest, InsertRequest__Output as _barq_InsertRequest__Output } from '../barq/InsertRequest';
import type { InsertResponse as _barq_InsertResponse, InsertResponse__Output as _barq_InsertResponse__Output } from '../barq/InsertResponse';
import type { SearchRequest as _barq_SearchRequest, SearchRequest__Output as _barq_SearchRequest__Output } from '../barq/SearchRequest';
import type { SearchResponse as _barq_SearchResponse, SearchResponse__Output as _barq_SearchResponse__Output } from '../barq/SearchResponse';
import type { StatusRequest as _barq_StatusRequest, StatusRequest__Output as _barq_StatusRequest__Output } from '../barq/StatusRequest';
import type { StatusResponse as _barq_StatusResponse, StatusResponse__Output as _barq_StatusResponse__Output } from '../barq/StatusResponse';

export interface BarqClient extends grpc.Client {
  BatchSearch(argument: _barq_BatchSearchRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_BatchSearchResponse__Output>): grpc.ClientUnaryCall;
  BatchSearch(argument: _barq_BatchSearchRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_barq_BatchSearchResponse__Output>): grpc.ClientUnaryCall;
  BatchSearch(argument: _barq_BatchSearchRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_BatchSearchResponse__Output>): grpc.ClientUnaryCall;
  BatchSearch(argument: _barq_BatchSearchRequest, callback: grpc.requestCallback<_barq_BatchSearchResponse__Output>): grpc.ClientUnaryCall;
  batchSearch(argument: _barq_BatchSearchRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_BatchSearchResponse__Output>): grpc.ClientUnaryCall;
  batchSearch(argument: _barq_BatchSearchRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_barq_BatchSearchResponse__Output>): grpc.ClientUnaryCall;
  batchSearch(argument: _barq_BatchSearchRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_BatchSearchResponse__Output>): grpc.ClientUnaryCall;
  batchSearch(argument: _barq_BatchSearchRequest, callback: grpc.requestCallback<_barq_BatchSearchResponse__Output>): grpc.ClientUnaryCall;
  
  CreateCollection(argument: _barq_CreateCollectionRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_CreateCollectionResponse__Output>): grpc.ClientUnaryCall;
  CreateCollection(argument: _barq_CreateCollectionRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_barq_CreateCollectionResponse__Output>): grpc.ClientUnaryCall;
  CreateCollection(argument: _barq_CreateCollectionRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_CreateCollectionResponse__Output>): grpc.ClientUnaryCall;
  CreateCollection(argument: _barq_CreateCollectionRequest, callback: grpc.requestCallback<_barq_CreateCollectionResponse__Output>): grpc.ClientUnaryCall;
  createCollection(argument: _barq_CreateCollectionRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_CreateCollectionResponse__Output>): grpc.ClientUnaryCall;
  createCollection(argument: _barq_CreateCollectionRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_barq_CreateCollectionResponse__Output>): grpc.ClientUnaryCall;
  createCollection(argument: _barq_CreateCollectionRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_CreateCollectionResponse__Output>): grpc.ClientUnaryCall;
  createCollection(argument: _barq_CreateCollectionRequest, callback: grpc.requestCallback<_barq_CreateCollectionResponse__Output>): grpc.ClientUnaryCall;
  
  Health(argument: _barq_HealthRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_HealthResponse__Output>): grpc.ClientUnaryCall;
  Health(argument: _barq_HealthRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_barq_HealthResponse__Output>): grpc.ClientUnaryCall;
  Health(argument: _barq_HealthRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_HealthResponse__Output>): grpc.ClientUnaryCall;
  Health(argument: _barq_HealthRequest, callback: grpc.requestCallback<_barq_HealthResponse__Output>): grpc.ClientUnaryCall;
  health(argument: _barq_HealthRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_HealthResponse__Output>): grpc.ClientUnaryCall;
  health(argument: _barq_HealthRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_barq_HealthResponse__Output>): grpc.ClientUnaryCall;
  health(argument: _barq_HealthRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_HealthResponse__Output>): grpc.ClientUnaryCall;
  health(argument: _barq_HealthRequest, callback: grpc.requestCallback<_barq_HealthResponse__Output>): grpc.ClientUnaryCall;
  
  Insert(argument: _barq_InsertRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_InsertResponse__Output>): grpc.ClientUnaryCall;
  Insert(argument: _barq_InsertRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_barq_InsertResponse__Output>): grpc.ClientUnaryCall;
  Insert(argument: _barq_InsertRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_InsertResponse__Output>): grpc.ClientUnaryCall;
  Insert(argument: _barq_InsertRequest, callback: grpc.requestCallback<_barq_InsertResponse__Output>): grpc.ClientUnaryCall;
  insert(argument: _barq_InsertRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_InsertResponse__Output>): grpc.ClientUnaryCall;
  insert(argument: _barq_InsertRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_barq_InsertResponse__Output>): grpc.ClientUnaryCall;
  insert(argument: _barq_InsertRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_InsertResponse__Output>): grpc.ClientUnaryCall;
  insert(argument: _barq_InsertRequest, callback: grpc.requestCallback<_barq_InsertResponse__Output>): grpc.ClientUnaryCall;
  
  InsertAsync(argument: _barq_InsertRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_InsertAsyncResponse__Output>): grpc.ClientUnaryCall;
  InsertAsync(argument: _barq_InsertRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_barq_InsertAsyncResponse__Output>): grpc.ClientUnaryCall;
  InsertAsync(argument: _barq_InsertRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_InsertAsyncResponse__Output>): grpc.ClientUnaryCall;
  InsertAsync(argument: _barq_InsertRequest, callback: grpc.requestCallback<_barq_InsertAsyncResponse__Output>): grpc.ClientUnaryCall;
  insertAsync(argument: _barq_InsertRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_InsertAsyncResponse__Output>): grpc.ClientUnaryCall;
  insertAsync(argument: _barq_InsertRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_barq_InsertAsyncResponse__Output>): grpc.ClientUnaryCall;
  insertAsync(argument: _barq_InsertRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_InsertAsyncResponse__Output>): grpc.ClientUnaryCall;
  insertAsync(argument: _barq_InsertRequest, callback: grpc.requestCallback<_barq_InsertAsyncResponse__Output>): grpc.ClientUnaryCall;
  
  InsertDocument(argument: _barq_InsertDocumentRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_InsertDocumentResponse__Output>): grpc.ClientUnaryCall;
  InsertDocument(argument: _barq_InsertDocumentRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_barq_InsertDocumentResponse__Output>): grpc.ClientUnaryCall;
  InsertDocument(argument: _barq_InsertDocumentRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_InsertDocumentResponse__Output>): grpc.ClientUnaryCall;
  InsertDocument(argument: _barq_InsertDocumentRequest, callback: grpc.requestCallback<_barq_InsertDocumentResponse__Output>): grpc.ClientUnaryCall;
  insertDocument(argument: _barq_InsertDocumentRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_InsertDocumentResponse__Output>): grpc.ClientUnaryCall;
  insertDocument(argument: _barq_InsertDocumentRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_barq_InsertDocumentResponse__Output>): grpc.ClientUnaryCall;
  insertDocument(argument: _barq_InsertDocumentRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_InsertDocumentResponse__Output>): grpc.ClientUnaryCall;
  insertDocument(argument: _barq_InsertDocumentRequest, callback: grpc.requestCallback<_barq_InsertDocumentResponse__Output>): grpc.ClientUnaryCall;
  
  Search(argument: _barq_SearchRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_SearchResponse__Output>): grpc.ClientUnaryCall;
  Search(argument: _barq_SearchRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_barq_SearchResponse__Output>): grpc.ClientUnaryCall;
  Search(argument: _barq_SearchRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_SearchResponse__Output>): grpc.ClientUnaryCall;
  Search(argument: _barq_SearchRequest, callback: grpc.requestCallback<_barq_SearchResponse__Output>): grpc.ClientUnaryCall;
  search(argument: _barq_SearchRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_SearchResponse__Output>): grpc.ClientUnaryCall;
  search(argument: _barq_SearchRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_barq_SearchResponse__Output>): grpc.ClientUnaryCall;
  search(argument: _barq_SearchRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_SearchResponse__Output>): grpc.ClientUnaryCall;
  search(argument: _barq_SearchRequest, callback: grpc.requestCallback<_barq_SearchResponse__Output>): grpc.ClientUnaryCall;
  
  Status(argument: _barq_StatusRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_StatusResponse__Output>): grpc.ClientUnaryCall;
  Status(argument: _barq_StatusRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_barq_StatusResponse__Output>): grpc.ClientUnaryCall;
  Status(argument: _barq_StatusRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_StatusResponse__Output>): grpc.ClientUnaryCall;
  Status(argument: _barq_StatusRequest, callback: grpc.requestCallback<_barq_StatusResponse__Output>): grpc.ClientUnaryCall;
  status(argument: _barq_StatusRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_StatusResponse__Output>): grpc.ClientUnaryCall;
  status(argument: _barq_StatusRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_barq_StatusResponse__Output>): grpc.ClientUnaryCall;
  status(argument: _barq_StatusRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_barq_StatusResponse__Output>): grpc.ClientUnaryCall;
  status(argument: _barq_StatusRequest, callback: grpc.requestCallback<_barq_StatusResponse__Output>): grpc.ClientUnaryCall;
  
}

export interface BarqHandlers extends grpc.UntypedServiceImplementation {
  BatchSearch: grpc.handleUnaryCall<_barq_BatchSearchRequest__Output, _barq_BatchSearchResponse>;
  
  CreateCollection: grpc.handleUnaryCall<_barq_CreateCollectionRequest__Output, _barq_CreateCollectionResponse>;
  
  Health: grpc.handleUnaryCall<_barq_HealthRequest__Output, _barq_HealthResponse>;
  
  Insert: grpc.handleUnaryCall<_barq_InsertRequest__Output, _barq_InsertResponse>;
  
  InsertAsync: grpc.handleUnaryCall<_barq_InsertRequest__Output, _barq_InsertAsyncResponse>;
  
  InsertDocument: grpc.handleUnaryCall<_barq_InsertDocumentRequest__Output, _barq_InsertDocumentResponse>;
  
  Search: grpc.handleUnaryCall<_barq_SearchRequest__Output, _barq_SearchResponse>;
  
  Status: grpc.handleUnaryCall<_barq_StatusRequest__Output, _barq_StatusResponse>;
  
}

export interface BarqDefinition extends grpc.ServiceDefinition {
  BatchSearch: MethodDefinition<_barq_BatchSearchRequest, _barq_BatchSearchResponse, _barq_BatchSearchRequest__Output, _barq_BatchSearchResponse__Output>
  CreateCollection: MethodDefinition<_barq_CreateCollectionRequest, _barq_CreateCollectionResponse, _barq_CreateCollectionRequest__Output, _barq_CreateCollectionResponse__Output>
  Health: MethodDefinition<_barq_HealthRequest, _barq_HealthResponse, _barq_HealthRequest__Output, _barq_HealthResponse__Output>
  Insert: MethodDefinition<_barq_InsertRequest, _barq_InsertResponse, _barq_InsertRequest__Output, _barq_InsertResponse__Output>
  InsertAsync: MethodDefinition<_barq_InsertRequest, _barq_InsertAsyncResponse, _barq_InsertRequest__Output, _barq_InsertAsyncResponse__Output>
  InsertDocument: MethodDefinition<_barq_InsertDocumentRequest, _barq_InsertDocumentResponse, _barq_InsertDocumentRequest__Output, _barq_InsertDocumentResponse__Output>
  Search: MethodDefinition<_barq_SearchRequest, _barq_SearchResponse, _barq_SearchRequest__Output, _barq_SearchResponse__Output>
  Status: MethodDefinition<_barq_StatusRequest, _barq_StatusResponse, _barq_StatusRequest__Output, _barq_StatusResponse__Output>
}
