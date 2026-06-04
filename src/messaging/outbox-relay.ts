import { Kafka, Producer } from 'kafkajs';
import { db } from '../database/connection';
import { config, logger } from '../config';

/**
 * Outbox Relay: Polls the transactional outbox table and publishes events to Kafka.
 * Guarantees at-least-once delivery. Kafka consumers must be idempotent.
 */
export class OutboxRelay {
  private producer: Producer;
  private running = false;
  private pollInterval: NodeJS.Timeout | null = null;
  private readonly batchSize = 100;
  private readonly pollMs = 500;

  constructor() {
    const kafka = new Kafka({
      clientId: config.kafka.clientId,
      brokers: config.kafka.brokers,
    });
    this.producer = kafka.producer({ idempotent: true });
  }

  async start() {
    await this.producer.connect();
    this.running = true;
    this.poll();
    logger.info('Outbox relay started');
  }

  async stop() {
    this.running = false;
    if (this.pollInterval) clearTimeout(this.pollInterval);
    await this.producer.disconnect();
    logger.info('Outbox relay stopped');
  }

  private poll() {
    if (!this.running) return;
    this.processBatch()
      .catch(err => logger.error(err, 'Outbox relay error'))
      .finally(() => {
        if (this.running) {
          this.pollInterval = setTimeout(() => this.poll(), this.pollMs);
        }
      });
  }

  private async processBatch() {
    // Fetch unpublished outbox entries with advisory lock to prevent duplicate processing
    const result = await db.query(
      `SELECT id, aggregate_type, aggregate_id, event_type, payload, created_at
       FROM outbox
       WHERE published = FALSE
       ORDER BY id ASC
       LIMIT $1
       FOR UPDATE SKIP LOCKED`,
      [this.batchSize]
    );

    if (result.rows.length === 0) return;

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
    await db.query(
      `UPDATE outbox SET published = TRUE, published_at = NOW() WHERE id = ANY($1)`,
      [ids]
    );

    logger.debug({ count: ids.length }, 'Outbox entries published to Kafka');
  }
}
