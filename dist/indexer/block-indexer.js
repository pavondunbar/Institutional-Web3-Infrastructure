"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.EventProcessor = exports.BlockIndexer = void 0;
const ethers_1 = require("ethers");
const connection_1 = require("../database/connection");
const redis_1 = require("../cache/redis");
const config_1 = require("../config");
const wallet_service_1 = require("../wallet/wallet-service");
/**
 * BlockIndexer: Ingests blocks from Ethereum-compatible chains.
 * Handles reorgs, indexes events, and triggers downstream processing.
 */
class BlockIndexer {
    provider;
    running = false;
    pollTimer = null;
    chain;
    confirmations;
    pollMs = 2000;
    walletService = new wallet_service_1.WalletService();
    constructor() {
        this.provider = new ethers_1.JsonRpcProvider(config_1.config.ethereum.rpcUrl);
        this.chain = `ethereum:${config_1.config.ethereum.chainId}`;
        this.confirmations = config_1.config.ethereum.confirmations;
    }
    async start() {
        this.running = true;
        this.poll();
        config_1.logger.info({ chain: this.chain }, 'Block indexer started');
    }
    async stop() {
        this.running = false;
        if (this.pollTimer)
            clearTimeout(this.pollTimer);
        config_1.logger.info('Block indexer stopped');
    }
    poll() {
        if (!this.running)
            return;
        this.indexNext()
            .catch(err => config_1.logger.error(err, 'Indexer error'))
            .finally(() => {
            if (this.running) {
                this.pollTimer = setTimeout(() => this.poll(), this.pollMs);
            }
        });
    }
    async indexNext() {
        const latestBlock = await this.provider.getBlockNumber();
        const safeBlock = latestBlock - this.confirmations;
        const lastIndexed = await redis_1.blockTracker.getHeight(this.chain);
        if (lastIndexed >= safeBlock)
            return; // caught up
        const nextHeight = lastIndexed + 1;
        const block = await this.provider.getBlock(nextHeight, true);
        if (!block)
            return;
        // Detect reorgs by checking parent hash
        if (lastIndexed > 0) {
            const reorged = await this.detectReorg(block);
            if (reorged)
                return; // reorg handled, will re-index from fork point
        }
        await this.processBlock(block);
        await redis_1.blockTracker.setHeight(this.chain, nextHeight);
        config_1.logger.debug({ block: nextHeight, txCount: block.transactions.length }, 'Block indexed');
    }
    async detectReorg(block) {
        const prevBlock = await connection_1.db.query(`SELECT block_hash FROM indexed_blocks
       WHERE chain = $1 AND block_number = $2 AND status = 'confirmed'`, [this.chain, block.number - 1]);
        if (prevBlock.rows.length === 0)
            return false;
        if (prevBlock.rows[0].block_hash !== block.parentHash) {
            config_1.logger.warn({ block: block.number, expectedParent: prevBlock.rows[0].block_hash, gotParent: block.parentHash }, 'Reorg detected');
            await this.handleReorg(block.number - 1);
            return true;
        }
        return false;
    }
    async handleReorg(forkPoint) {
        // Mark reorged blocks
        await connection_1.db.query(`UPDATE indexed_blocks SET status = 'reorged' WHERE chain = $1 AND block_number > $2`, [this.chain, forkPoint]);
        // Mark events from reorged blocks as unprocessed
        await connection_1.db.query(`UPDATE indexed_events SET processed = FALSE
       WHERE block_id IN (
         SELECT id FROM indexed_blocks WHERE chain = $1 AND block_number > $2 AND status = 'reorged'
       )`, [this.chain, forkPoint]);
        // Handle reorged transactions in wallet service
        await this.walletService.handleReorg(forkPoint, this.chain);
        // Reset indexer to fork point
        await redis_1.blockTracker.setHeight(this.chain, forkPoint);
        // Emit reorg event
        await connection_1.db.query(`INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
       VALUES ('indexer', $1, 'chain.reorg', $2)`, [this.chain, JSON.stringify({ chain: this.chain, forkPoint })]);
        config_1.logger.warn({ chain: this.chain, forkPoint }, 'Reorg handled, re-indexing from fork point');
    }
    async processBlock(block) {
        // Insert block record
        const blockResult = await connection_1.db.query(`INSERT INTO indexed_blocks (chain, block_number, block_hash, parent_hash, timestamp, tx_count)
       VALUES ($1, $2, $3, $4, to_timestamp($5), $6)
       ON CONFLICT (chain, block_number) WHERE status = 'confirmed' DO NOTHING
       RETURNING id`, [this.chain, block.number, block.hash, block.parentHash, block.timestamp, block.transactions.length]);
        if (blockResult.rows.length === 0)
            return; // already indexed
        const blockId = blockResult.rows[0].id;
        // Fetch all logs for this block
        const logs = await this.provider.getLogs({ blockHash: block.hash });
        await this.indexEvents(blockId, logs);
    }
    async indexEvents(blockId, logs) {
        if (logs.length === 0)
            return;
        const values = [];
        const params = [];
        let paramIdx = 1;
        for (const log of logs) {
            values.push(`($${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++})`);
            params.push(blockId, this.chain, log.transactionHash, log.index, log.address, log.topics[0] || '', JSON.stringify(log.topics), log.data);
        }
        await connection_1.db.query(`INSERT INTO indexed_events (block_id, chain, tx_hash, log_index, contract_address, event_signature, topics, data)
       VALUES ${values.join(', ')}`, params);
    }
}
exports.BlockIndexer = BlockIndexer;
/**
 * EventProcessor: Processes indexed events and triggers business logic.
 * Runs as a separate process to decouple indexing from processing.
 */
class EventProcessor {
    running = false;
    pollTimer = null;
    batchSize = 50;
    pollMs = 1000;
    handlers = new Map();
    registerHandler(eventSignature, handler) {
        this.handlers.set(eventSignature, handler);
    }
    async start() {
        this.running = true;
        this.poll();
        config_1.logger.info('Event processor started');
    }
    stop() {
        this.running = false;
        if (this.pollTimer)
            clearTimeout(this.pollTimer);
    }
    poll() {
        if (!this.running)
            return;
        this.processBatch()
            .catch(err => config_1.logger.error(err, 'Event processor error'))
            .finally(() => {
            if (this.running) {
                this.pollTimer = setTimeout(() => this.poll(), this.pollMs);
            }
        });
    }
    async processBatch() {
        const result = await connection_1.db.query(`SELECT e.*, b.block_number, b.block_hash
       FROM indexed_events e
       JOIN indexed_blocks b ON e.block_id = b.id
       WHERE e.processed = FALSE AND b.status = 'confirmed'
       ORDER BY e.id ASC
       LIMIT $1
       FOR UPDATE OF e SKIP LOCKED`, [this.batchSize]);
        for (const row of result.rows) {
            const handler = this.handlers.get(row.event_signature);
            if (handler) {
                await handler({
                    id: row.id,
                    chain: row.chain,
                    txHash: row.tx_hash,
                    logIndex: row.log_index,
                    contractAddress: row.contract_address,
                    eventSignature: row.event_signature,
                    topics: row.topics,
                    data: row.data,
                    blockNumber: row.block_number,
                    blockHash: row.block_hash,
                });
            }
            await connection_1.db.query('UPDATE indexed_events SET processed = TRUE WHERE id = $1', [row.id]);
        }
    }
}
exports.EventProcessor = EventProcessor;
//# sourceMappingURL=block-indexer.js.map