// Original file: proto/barq.proto

export const ClusterMode = {
  CLUSTER_MODE_UNSPECIFIED: 0,
  CLUSTER_MODE_SINGLE_NODE: 1,
  CLUSTER_MODE_ROUTED_REPLICATION: 2,
  CLUSTER_MODE_CONSENSUS_BACKED: 3,
} as const;

export type ClusterMode =
  | 'CLUSTER_MODE_UNSPECIFIED'
  | 0
  | 'CLUSTER_MODE_SINGLE_NODE'
  | 1
  | 'CLUSTER_MODE_ROUTED_REPLICATION'
  | 2
  | 'CLUSTER_MODE_CONSENSUS_BACKED'
  | 3

export type ClusterMode__Output = typeof ClusterMode[keyof typeof ClusterMode]
