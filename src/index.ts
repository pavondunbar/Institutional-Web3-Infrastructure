import { config, logger } from './config';
import { db } from './database/connection';
import { redis } from './cache/redis';
import { OutboxRelay } from './messaging/outbox-relay';
import { BlockIndexer } from './indexer/block-indexer';
import { ReconciliationService } from './reconciliation/reconciliation-service';
import { createApp } from './api/app';

async function main() {
  logger.info('Starting TradFi-Web3 Infrastructure...');

  // Start API server
  const app = createApp();
  app.listen(config.server.port, () => {
    logger.info({ port: config.server.port }, 'API server listening');
  });

  // Start outbox relay (publishes to Kafka)
  const relay = new OutboxRelay();
  await relay.start();

  // Start block indexer
  const indexer = new BlockIndexer();
  await indexer.start();

  // Start reconciliation jobs
  const reconciler = new ReconciliationService();
  reconciler.start();

  // Graceful shutdown
  const shutdown = async () => {
    logger.info('Shutting down...');
    await relay.stop();
    await indexer.stop();
    reconciler.stop();
    await redis.quit();
    await db.end();
    process.exit(0);
  };

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

main().catch((err) => {
  logger.fatal(err, 'Fatal startup error');
  process.exit(1);
});
