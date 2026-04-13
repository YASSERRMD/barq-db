// Original file: proto/barq.proto

export const Consistency = {
  CONSISTENCY_UNSPECIFIED: 0,
  CONSISTENCY_PRIMARY: 1,
  CONSISTENCY_FOLLOWERS: 2,
  CONSISTENCY_ANY: 3,
} as const;

export type Consistency =
  | 'CONSISTENCY_UNSPECIFIED'
  | 0
  | 'CONSISTENCY_PRIMARY'
  | 1
  | 'CONSISTENCY_FOLLOWERS'
  | 2
  | 'CONSISTENCY_ANY'
  | 3

export type Consistency__Output = typeof Consistency[keyof typeof Consistency]
