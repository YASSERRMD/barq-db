// Original file: proto/barq.proto

import type { InsertStatusState as _barq_InsertStatusState, InsertStatusState__Output as _barq_InsertStatusState__Output } from '../barq/InsertStatusState';

export interface GetInsertStatusResponse {
  'requestId'?: (string);
  'state'?: (_barq_InsertStatusState);
  'errorMessage'?: (string);
}

export interface GetInsertStatusResponse__Output {
  'requestId'?: (string);
  'state'?: (_barq_InsertStatusState__Output);
  'errorMessage'?: (string);
}
