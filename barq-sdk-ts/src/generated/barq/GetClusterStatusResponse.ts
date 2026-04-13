// Original file: proto/barq.proto

import type { ClusterMode as _barq_ClusterMode, ClusterMode__Output as _barq_ClusterMode__Output } from '../barq/ClusterMode';
import type { WriteDurability as _barq_WriteDurability, WriteDurability__Output as _barq_WriteDurability__Output } from '../barq/WriteDurability';
import type { Long } from '@grpc/proto-loader';

export interface GetClusterStatusResponse {
  'nodeId'?: (string);
  'mode'?: (_barq_ClusterMode);
  'writeDurability'?: (_barq_WriteDurability);
  'shardCount'?: (number);
  'nodeCount'?: (number | string | Long);
}

export interface GetClusterStatusResponse__Output {
  'nodeId'?: (string);
  'mode'?: (_barq_ClusterMode__Output);
  'writeDurability'?: (_barq_WriteDurability__Output);
  'shardCount'?: (number);
  'nodeCount'?: (Long);
}
