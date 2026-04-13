// Original file: proto/barq.proto

import type { TenantMemorySample as _barq_TenantMemorySample, TenantMemorySample__Output as _barq_TenantMemorySample__Output } from '../barq/TenantMemorySample';
import type { CollectionMemorySample as _barq_CollectionMemorySample, CollectionMemorySample__Output as _barq_CollectionMemorySample__Output } from '../barq/CollectionMemorySample';
import type { CollectionWalSample as _barq_CollectionWalSample, CollectionWalSample__Output as _barq_CollectionWalSample__Output } from '../barq/CollectionWalSample';
import type { CollectionSegmentFileSample as _barq_CollectionSegmentFileSample, CollectionSegmentFileSample__Output as _barq_CollectionSegmentFileSample__Output } from '../barq/CollectionSegmentFileSample';
import type { CollectionSegmentStateSample as _barq_CollectionSegmentStateSample, CollectionSegmentStateSample__Output as _barq_CollectionSegmentStateSample__Output } from '../barq/CollectionSegmentStateSample';
import type { Long } from '@grpc/proto-loader';

export interface StorageMetrics {
  'refreshCount'?: (number | string | Long);
  'totalResidentVectorMemoryBytes'?: (number | string | Long);
  'walAppendsTotal'?: (number | string | Long);
  'walBytesWrittenTotal'?: (number | string | Long);
  'compactionsTotal'?: (number | string | Long);
  'tenantMemoryBytes'?: (_barq_TenantMemorySample)[];
  'collectionMemoryBytes'?: (_barq_CollectionMemorySample)[];
  'collectionWal'?: (_barq_CollectionWalSample)[];
  'collectionSegmentFiles'?: (_barq_CollectionSegmentFileSample)[];
  'collectionSegmentStates'?: (_barq_CollectionSegmentStateSample)[];
}

export interface StorageMetrics__Output {
  'refreshCount'?: (Long);
  'totalResidentVectorMemoryBytes'?: (Long);
  'walAppendsTotal'?: (Long);
  'walBytesWrittenTotal'?: (Long);
  'compactionsTotal'?: (Long);
  'tenantMemoryBytes'?: (_barq_TenantMemorySample__Output)[];
  'collectionMemoryBytes'?: (_barq_CollectionMemorySample__Output)[];
  'collectionWal'?: (_barq_CollectionWalSample__Output)[];
  'collectionSegmentFiles'?: (_barq_CollectionSegmentFileSample__Output)[];
  'collectionSegmentStates'?: (_barq_CollectionSegmentStateSample__Output)[];
}
