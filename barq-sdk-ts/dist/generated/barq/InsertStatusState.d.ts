export declare const InsertStatusState: {
    readonly INSERT_STATUS_STATE_UNSPECIFIED: 0;
    readonly INSERT_STATUS_STATE_QUEUED: 1;
    readonly INSERT_STATUS_STATE_PROCESSING: 2;
    readonly INSERT_STATUS_STATE_SUCCEEDED: 3;
    readonly INSERT_STATUS_STATE_FAILED: 4;
};
export type InsertStatusState = 'INSERT_STATUS_STATE_UNSPECIFIED' | 0 | 'INSERT_STATUS_STATE_QUEUED' | 1 | 'INSERT_STATUS_STATE_PROCESSING' | 2 | 'INSERT_STATUS_STATE_SUCCEEDED' | 3 | 'INSERT_STATUS_STATE_FAILED' | 4;
export type InsertStatusState__Output = typeof InsertStatusState[keyof typeof InsertStatusState];
