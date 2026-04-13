// Original file: proto/barq.proto


export interface InsertDocumentRequest {
  'collection'?: (string);
  'id'?: (string);
  'vector'?: (number | string)[];
  'payloadJson'?: (string);
}

export interface InsertDocumentRequest__Output {
  'collection'?: (string);
  'id'?: (string);
  'vector'?: (number)[];
  'payloadJson'?: (string);
}
