import type { SearchQuery as _barq_SearchQuery, SearchQuery__Output as _barq_SearchQuery__Output } from '../barq/SearchQuery';
export interface BatchSearchRequest {
    'collection'?: (string);
    'queries'?: (_barq_SearchQuery)[];
    'topK'?: (number);
}
export interface BatchSearchRequest__Output {
    'collection'?: (string);
    'queries'?: (_barq_SearchQuery__Output)[];
    'topK'?: (number);
}
