export declare const Consistency: {
    readonly CONSISTENCY_UNSPECIFIED: 0;
    readonly CONSISTENCY_PRIMARY: 1;
    readonly CONSISTENCY_FOLLOWERS: 2;
    readonly CONSISTENCY_ANY: 3;
};
export type Consistency = 'CONSISTENCY_UNSPECIFIED' | 0 | 'CONSISTENCY_PRIMARY' | 1 | 'CONSISTENCY_FOLLOWERS' | 2 | 'CONSISTENCY_ANY' | 3;
export type Consistency__Output = typeof Consistency[keyof typeof Consistency];
