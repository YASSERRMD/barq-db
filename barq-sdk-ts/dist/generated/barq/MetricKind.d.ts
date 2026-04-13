export declare const MetricKind: {
    readonly METRIC_KIND_UNSPECIFIED: 0;
    readonly METRIC_KIND_COUNTER: 1;
    readonly METRIC_KIND_GAUGE: 2;
    readonly METRIC_KIND_HISTOGRAM: 3;
};
export type MetricKind = 'METRIC_KIND_UNSPECIFIED' | 0 | 'METRIC_KIND_COUNTER' | 1 | 'METRIC_KIND_GAUGE' | 2 | 'METRIC_KIND_HISTOGRAM' | 3;
export type MetricKind__Output = typeof MetricKind[keyof typeof MetricKind];
