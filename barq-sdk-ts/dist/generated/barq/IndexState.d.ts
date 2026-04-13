export declare const IndexState: {
    readonly INDEX_STATE_UNSPECIFIED: 0;
    readonly INDEX_STATE_BUILDING: 1;
    readonly INDEX_STATE_READY: 2;
    readonly INDEX_STATE_STALE: 3;
};
export type IndexState = 'INDEX_STATE_UNSPECIFIED' | 0 | 'INDEX_STATE_BUILDING' | 1 | 'INDEX_STATE_READY' | 2 | 'INDEX_STATE_STALE' | 3;
export type IndexState__Output = typeof IndexState[keyof typeof IndexState];
