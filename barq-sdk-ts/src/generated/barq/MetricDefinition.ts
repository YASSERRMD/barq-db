// Original file: proto/barq.proto

import type { MetricKind as _barq_MetricKind, MetricKind__Output as _barq_MetricKind__Output } from '../barq/MetricKind';

export interface MetricDefinition {
  'name'?: (string);
  'kind'?: (_barq_MetricKind);
  'description'?: (string);
  'unit'?: (string);
  'labels'?: (string)[];
}

export interface MetricDefinition__Output {
  'name'?: (string);
  'kind'?: (_barq_MetricKind__Output);
  'description'?: (string);
  'unit'?: (string);
  'labels'?: (string)[];
}
