export interface SearchRequest {
    'collection'?: (string);
    'vector'?: (number | string)[];
    'topK'?: (number);
}
export interface SearchRequest__Output {
    'collection'?: (string);
    'vector'?: (number)[];
    'topK'?: (number);
}
