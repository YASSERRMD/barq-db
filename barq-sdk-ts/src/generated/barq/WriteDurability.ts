// Original file: proto/barq.proto

export const WriteDurability = {
  WRITE_DURABILITY_UNSPECIFIED: 0,
  WRITE_DURABILITY_NODE_LOCAL: 1,
  WRITE_DURABILITY_PRIMARY_ONLY: 2,
  WRITE_DURABILITY_CONSENSUS_QUORUM: 3,
} as const;

export type WriteDurability =
  | 'WRITE_DURABILITY_UNSPECIFIED'
  | 0
  | 'WRITE_DURABILITY_NODE_LOCAL'
  | 1
  | 'WRITE_DURABILITY_PRIMARY_ONLY'
  | 2
  | 'WRITE_DURABILITY_CONSENSUS_QUORUM'
  | 3

export type WriteDurability__Output = typeof WriteDurability[keyof typeof WriteDurability]
