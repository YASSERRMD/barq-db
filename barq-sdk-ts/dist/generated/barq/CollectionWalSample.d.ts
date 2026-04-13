import type { Long } from '@grpc/proto-loader';
export interface CollectionWalSample {
    'tenant'?: (string);
    'collection'?: (string);
    'entries'?: (number | string | Long);
    'bytes'?: (number | string | Long);
}
export interface CollectionWalSample__Output {
    'tenant'?: (string);
    'collection'?: (string);
    'entries'?: (Long);
    'bytes'?: (Long);
}
