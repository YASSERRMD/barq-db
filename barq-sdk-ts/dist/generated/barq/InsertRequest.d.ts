import type { InsertOptions as _barq_InsertOptions, InsertOptions__Output as _barq_InsertOptions__Output } from '../barq/InsertOptions';
export interface InsertRequest {
    'collection'?: (string);
    'id'?: (string);
    'vector'?: (number | string)[];
    'payloadJson'?: (string);
    'options'?: (_barq_InsertOptions | null);
}
export interface InsertRequest__Output {
    'collection'?: (string);
    'id'?: (string);
    'vector'?: (number)[];
    'payloadJson'?: (string);
    'options'?: (_barq_InsertOptions__Output);
}
