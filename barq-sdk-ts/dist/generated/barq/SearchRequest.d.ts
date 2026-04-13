import type { SearchOptions as _barq_SearchOptions, SearchOptions__Output as _barq_SearchOptions__Output } from '../barq/SearchOptions';
export interface SearchRequest {
    'collection'?: (string);
    'vector'?: (number | string)[];
    'topK'?: (number);
    'options'?: (_barq_SearchOptions | null);
}
export interface SearchRequest__Output {
    'collection'?: (string);
    'vector'?: (number)[];
    'topK'?: (number);
    'options'?: (_barq_SearchOptions__Output);
}
