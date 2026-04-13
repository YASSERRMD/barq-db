import type { SegmentState as _barq_SegmentState, SegmentState__Output as _barq_SegmentState__Output } from '../barq/SegmentState';
import type { Long } from '@grpc/proto-loader';
export interface SegmentCount {
    'state'?: (_barq_SegmentState);
    'count'?: (number | string | Long);
}
export interface SegmentCount__Output {
    'state'?: (_barq_SegmentState__Output);
    'count'?: (Long);
}
