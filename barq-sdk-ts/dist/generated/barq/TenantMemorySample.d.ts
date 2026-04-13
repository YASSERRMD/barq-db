import type { Long } from '@grpc/proto-loader';
export interface TenantMemorySample {
    'tenant'?: (string);
    'residentVectorMemoryBytes'?: (number | string | Long);
}
export interface TenantMemorySample__Output {
    'tenant'?: (string);
    'residentVectorMemoryBytes'?: (Long);
}
