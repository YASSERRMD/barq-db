import type { MetricDefinition as _barq_MetricDefinition, MetricDefinition__Output as _barq_MetricDefinition__Output } from '../barq/MetricDefinition';
import type { StorageMetrics as _barq_StorageMetrics, StorageMetrics__Output as _barq_StorageMetrics__Output } from '../barq/StorageMetrics';
export interface GetMetricsResponse {
    'definitions'?: (_barq_MetricDefinition)[];
    'storage'?: (_barq_StorageMetrics | null);
}
export interface GetMetricsResponse__Output {
    'definitions'?: (_barq_MetricDefinition__Output)[];
    'storage'?: (_barq_StorageMetrics__Output);
}
