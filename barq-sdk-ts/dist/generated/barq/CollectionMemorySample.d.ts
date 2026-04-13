import type { Long } from '@grpc/proto-loader';
export interface CollectionMemorySample {
    'tenant'?: (string);
    'collection'?: (string);
    'residentVectorMemoryBytes'?: (number | string | Long);
}
export interface CollectionMemorySample__Output {
    'tenant'?: (string);
    'collection'?: (string);
    'residentVectorMemoryBytes'?: (Long);
}
