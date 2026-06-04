/**
 * Outbox Relay: Polls the transactional outbox table and publishes events to Kafka.
 * Guarantees at-least-once delivery. Kafka consumers must be idempotent.
 */
export declare class OutboxRelay {
    private producer;
    private running;
    private pollInterval;
    private readonly batchSize;
    private readonly pollMs;
    constructor();
    start(): Promise<void>;
    stop(): Promise<void>;
    private poll;
    private processBatch;
}
//# sourceMappingURL=outbox-relay.d.ts.map