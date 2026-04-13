// Original file: proto/barq.proto

export const InsertStatusState = {
  INSERT_STATUS_STATE_UNSPECIFIED: 0,
  INSERT_STATUS_STATE_QUEUED: 1,
  INSERT_STATUS_STATE_PROCESSING: 2,
  INSERT_STATUS_STATE_SUCCEEDED: 3,
  INSERT_STATUS_STATE_FAILED: 4,
} as const;

export type InsertStatusState =
  | 'INSERT_STATUS_STATE_UNSPECIFIED'
  | 0
  | 'INSERT_STATUS_STATE_QUEUED'
  | 1
  | 'INSERT_STATUS_STATE_PROCESSING'
  | 2
  | 'INSERT_STATUS_STATE_SUCCEEDED'
  | 3
  | 'INSERT_STATUS_STATE_FAILED'
  | 4

export type InsertStatusState__Output = typeof InsertStatusState[keyof typeof InsertStatusState]
