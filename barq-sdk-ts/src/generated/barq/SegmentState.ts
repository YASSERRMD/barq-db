// Original file: proto/barq.proto

export const SegmentState = {
  SEGMENT_STATE_UNSPECIFIED: 0,
  SEGMENT_STATE_GROWING: 1,
  SEGMENT_STATE_SEALED: 2,
  SEGMENT_STATE_COMPACTED: 3,
} as const;

export type SegmentState =
  | 'SEGMENT_STATE_UNSPECIFIED'
  | 0
  | 'SEGMENT_STATE_GROWING'
  | 1
  | 'SEGMENT_STATE_SEALED'
  | 2
  | 'SEGMENT_STATE_COMPACTED'
  | 3

export type SegmentState__Output = typeof SegmentState[keyof typeof SegmentState]
