"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.OutboxRelay = void 0;
const kafkajs_1 = require("kafkajs");
const connection_1 = require("../database/connection");
const config_1 = require("../config");
/**
 * Outbox Relay: Polls the transactional outbox table and publishes events to Kafka.
 * Guarantees at-least-once delivery. Kafka consumers must be idempotent.
 */
class OutboxRelay {
    producer;
    running = false;
    pollInterval = null;
    batchSize = 100;
    pollMs = 500;
    constructor() {
        const kafka = new kafkajs_1.Kafka({
            clientId: config_1.config.kafka.clientId,
            brokers: config_1.config.kafka.brokers,
        });
        this.producer = kafka.producer({ idempotent: true });
    }
    async start() {
        await this.producer.connect();
        this.running = true;
        this.poll();
        config_1.logger.info('Outbox relay started');
    }
    async stop() {
        this.running = false;
        if (this.pollInterval)
            clearTimeout(this.pollInterval);
        await this.producer.disconnect();
        config_1.logger.info('Outbox relay stopped');
    }
    poll() {
        if (!this.running)
            return;
        this.processBatch()
            .catch(err => config_1.logger.error(err, 'Outbox relay error'))
            .finally(() => {
            if (this.running) {
                this.pollInterval = setTimeout(() => this.poll(), this.pollMs);
            }
        });
    }
    async processBatch() {
        // Fetch unpublished outbox entries with advisory lock to prevent duplicate processing
        const result = await connection_1.db.query(`SELECT id, aggregate_type, aggregate_id, event_type, payload, created_at
       FROM outbox
       WHERE published = FALSE
       ORDER BY id ASC
       LIMIT $1
       FOR UPDATE SKIP LOCKED`, [this.batchSize]);
        if (result.rows.length === 0)
            return;
        const messages = result.rows.map(row => ({
            topic: `${row.aggregate_type}.events`,
            messages: [{
                    key: row.aggregate_id,
                    value: JSON.stringify({
                        eventId: row.id,
                        eventType: row.event_type,
                        aggregateType: row.aggregate_type,
                        aggregateId: row.aggregate_id,
                        payload: row.payload,
                        timestamp: row.created_at,
                    }),
                    headers: {
                        'event-type': row.event_type,
                        'aggregate-id': row.aggregate_id,
                    },
                }],
        }));
        // Publish to Kafka
        await this.producer.sendBatch({ topicMessages: messages });
        // Mark as published
        const ids = result.rows.map(r => r.id);
        await connection_1.db.query(`UPDATE outbox SET published = TRUE, published_at = NOW() WHERE id = ANY($1)`, [ids]);
        config_1.logger.debug({ count: ids.length }, 'Outbox entries published to Kafka');
    }
}
exports.OutboxRelay = OutboxRelay;
//# sourceMappingURL=outbox-relay.js.map