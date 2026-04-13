export declare const WriteDurability: {
    readonly WRITE_DURABILITY_UNSPECIFIED: 0;
    readonly WRITE_DURABILITY_NODE_LOCAL: 1;
    readonly WRITE_DURABILITY_PRIMARY_ONLY: 2;
    readonly WRITE_DURABILITY_CONSENSUS_QUORUM: 3;
};
export type WriteDurability = 'WRITE_DURABILITY_UNSPECIFIED' | 0 | 'WRITE_DURABILITY_NODE_LOCAL' | 1 | 'WRITE_DURABILITY_PRIMARY_ONLY' | 2 | 'WRITE_DURABILITY_CONSENSUS_QUORUM' | 3;
export type WriteDurability__Output = typeof WriteDurability[keyof typeof WriteDurability];
