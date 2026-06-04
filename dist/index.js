"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const config_1 = require("./config");
const connection_1 = require("./database/connection");
const redis_1 = require("./cache/redis");
const outbox_relay_1 = require("./messaging/outbox-relay");
const block_indexer_1 = require("./indexer/block-indexer");
const reconciliation_service_1 = require("./reconciliation/reconciliation-service");
const app_1 = require("./api/app");
async function main() {
    config_1.logger.info('Starting TradFi-Web3 Infrastructure...');
    // Start API server
    const app = (0, app_1.createApp)();
    app.listen(config_1.config.server.port, () => {
        config_1.logger.info({ port: config_1.config.server.port }, 'API server listening');
    });
    // Start outbox relay (publishes to Kafka)
    const relay = new outbox_relay_1.OutboxRelay();
    await relay.start();
    // Start block indexer
    const indexer = new block_indexer_1.BlockIndexer();
    await indexer.start();
    // Start reconciliation jobs
    const reconciler = new reconciliation_service_1.ReconciliationService();
    reconciler.start();
    // Graceful shutdown
    const shutdown = async () => {
        config_1.logger.info('Shutting down...');
        await relay.stop();
        await indexer.stop();
        reconciler.stop();
        await redis_1.redis.quit();
        await connection_1.db.end();
        process.exit(0);
    };
    process.on('SIGTERM', shutdown);
    process.on('SIGINT', shutdown);
}
main().catch((err) => {
    config_1.logger.fatal(err, 'Fatal startup error');
    process.exit(1);
});
//# sourceMappingURL=index.js.map