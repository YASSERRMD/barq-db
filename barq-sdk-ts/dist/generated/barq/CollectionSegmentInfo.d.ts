import type { SegmentState as _barq_SegmentState, SegmentState__Output as _barq_SegmentState__Output } from '../barq/SegmentState';
import type { IndexState as _barq_IndexState, IndexState__Output as _barq_IndexState__Output } from '../barq/IndexState';
import type { SegmentCount as _barq_SegmentCount, SegmentCount__Output as _barq_SegmentCount__Output } from '../barq/SegmentCount';
export interface CollectionSegmentInfo {
    'tenant'?: (string);
    'collection'?: (string);
    'currentState'?: (_barq_SegmentState);
    'indexState'?: (_barq_IndexState);
    'segmentCounts'?: (_barq_SegmentCount)[];
}
export interface CollectionSegmentInfo__Output {
    'tenant'?: (string);
    'collection'?: (string);
    'currentState'?: (_barq_SegmentState__Output);
    'indexState'?: (_barq_IndexState__Output);
    'segmentCounts'?: (_barq_SegmentCount__Output)[];
}
