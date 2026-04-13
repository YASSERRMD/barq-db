// Original file: proto/barq.proto

import type { Consistency as _barq_Consistency, Consistency__Output as _barq_Consistency__Output } from '../barq/Consistency';

export interface SearchOptions {
  'consistency'?: (_barq_Consistency);
  'allowFallback'?: (boolean);
}

export interface SearchOptions__Output {
  'consistency'?: (_barq_Consistency__Output);
  'allowFallback'?: (boolean);
}
