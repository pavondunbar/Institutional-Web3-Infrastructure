"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.createApp = createApp;
const express_1 = __importDefault(require("express"));
const uuid_1 = require("uuid");
const ledger_service_1 = require("../database/ledger-service");
const wallet_service_1 = require("../wallet/wallet-service");
const chain_service_1 = require("../chain/chain-service");
const connection_1 = require("../database/connection");
const redis_1 = require("../cache/redis");
const config_1 = require("../config");
function param(req, name) {
    const val = req.params[name];
    return Array.isArray(val) ? val[0] : val;
}
function createApp() {
    const app = (0, express_1.default)();
    app.use(express_1.default.json());
    const walletService = new wallet_service_1.WalletService();
    const chainService = new chain_service_1.ChainService();
    // Rate limiting middleware
    app.use(async (req, res, next) => {
        const key = (Array.isArray(req.ip) ? req.ip[0] : req.ip) || 'unknown';
        const allowed = await redis_1.rateLimiter.check(key, 100, 60);
        if (!allowed)
            return res.status(429).json({ error: 'Rate limit exceeded' });
        next();
    });
    // ======================== HEALTH ========================
    app.get('/health', (_req, res) => {
        res.json({ status: 'ok', timestamp: new Date().toISOString() });
    });
    // ======================== LEDGER ========================
    app.post('/api/v1/journal', async (req, res) => {
        try {
            const { idempotencyKey, description, externalRef, externalRefType, lines, metadata } = req.body;
            if (!idempotencyKey || !lines || lines.length === 0) {
                return res.status(400).json({ error: 'idempotencyKey and lines are required' });
            }
            const result = await (0, ledger_service_1.postJournal)({
                idempotencyKey,
                description,
                externalRef,
                externalRefType,
                lines: lines.map((l) => ({
                    accountId: l.accountId,
                    amount: BigInt(l.amount),
                    direction: l.direction,
                })),
                metadata,
            });
            res.status(201).json({
                journalEntryId: result.journalEntryId,
                entries: result.entries.map(e => ({ ...e, amount: e.amount.toString() })),
            });
        }
        catch (err) {
            const msg = err instanceof Error ? err.message : 'Unknown error';
            config_1.logger.error(err, 'Journal posting failed');
            res.status(400).json({ error: msg });
        }
    });
    app.post('/api/v1/journal/:id/reverse', async (req, res) => {
        try {
            const idempotencyKey = req.body.idempotencyKey || (0, uuid_1.v4)();
            const result = await (0, ledger_service_1.reverseJournal)(param(req, 'id'), idempotencyKey);
            res.json({ journalEntryId: result.journalEntryId, entries: result.entries.map(e => ({ ...e, amount: e.amount.toString() })) });
        }
        catch (err) {
            const msg = err instanceof Error ? err.message : 'Unknown error';
            res.status(400).json({ error: msg });
        }
    });
    app.get('/api/v1/accounts/:id/balance', async (req, res) => {
        try {
            const balance = await (0, ledger_service_1.getBalance)(param(req, 'id'));
            if (!balance)
                return res.status(404).json({ error: 'Account not found' });
            res.json({
                accountId: balance.accountId,
                debitTotal: balance.debitTotal.toString(),
                creditTotal: balance.creditTotal.toString(),
                balance: balance.balance.toString(),
                lastEntryHash: balance.lastEntryHash,
            });
        }
        catch (err) {
            res.status(500).json({ error: 'Internal error' });
        }
    });
    app.get('/api/v1/accounts/:id/history', async (req, res) => {
        try {
            const limit = Math.min(parseInt(String(req.query.limit) || '50'), 200);
            const offset = parseInt(String(req.query.offset) || '0');
            const result = await connection_1.db.query(`SELECT l.*, j.description, j.external_ref, j.status as journal_status
         FROM ledger_entries l JOIN journal_entries j ON l.journal_entry_id = j.id
         WHERE l.account_id = $1 ORDER BY l.sequence_num DESC LIMIT $2 OFFSET $3`, [param(req, 'id'), limit, offset]);
            res.json({ entries: result.rows, limit, offset });
        }
        catch (err) {
            res.status(500).json({ error: 'Internal error' });
        }
    });
    // ======================== WALLETS ========================
    app.post('/api/v1/wallets', async (req, res) => {
        try {
            const id = await walletService.createWallet(req.body);
            res.status(201).json({ walletId: id });
        }
        catch (err) {
            const msg = err instanceof Error ? err.message : 'Unknown error';
            res.status(400).json({ error: msg });
        }
    });
    app.get('/api/v1/wallets/:id', async (req, res) => {
        const wallet = await walletService.getWallet(param(req, 'id'));
        if (!wallet)
            return res.status(404).json({ error: 'Wallet not found' });
        res.json(wallet);
    });
    app.post('/api/v1/wallets/:id/transactions', async (req, res) => {
        try {
            const txId = await walletService.createTransaction({
                walletId: param(req, 'id'),
                toAddress: req.body.toAddress,
                amount: BigInt(req.body.amount),
                gasLimit: req.body.gasLimit ? BigInt(req.body.gasLimit) : undefined,
                gasPrice: req.body.gasPrice ? BigInt(req.body.gasPrice) : undefined,
                metadata: req.body.metadata,
            });
            res.status(201).json({ transactionId: txId });
        }
        catch (err) {
            const msg = err instanceof Error ? err.message : 'Unknown error';
            res.status(400).json({ error: msg });
        }
    });
    // ======================== CHAIN ========================
    app.get('/api/v1/chain/state', async (_req, res) => {
        try {
            const state = await chainService.getChainState();
            res.json(state);
        }
        catch (err) {
            res.status(500).json({ error: 'Failed to get chain state' });
        }
    });
    app.get('/api/v1/chain/tx/:hash', async (req, res) => {
        try {
            const status = await chainService.getTransactionStatus(param(req, 'hash'));
            res.json(status);
        }
        catch (err) {
            res.status(500).json({ error: 'Failed to get tx status' });
        }
    });
    app.post('/api/v1/chain/estimate-gas', async (req, res) => {
        try {
            const estimate = await chainService.estimateGas(req.body.to, BigInt(req.body.value || '0'), req.body.data);
            res.json({
                gasLimit: estimate.gasLimit.toString(),
                gasPrice: estimate.gasPrice.toString(),
                maxFeePerGas: estimate.maxFeePerGas.toString(),
                maxPriorityFeePerGas: estimate.maxPriorityFeePerGas.toString(),
            });
        }
        catch (err) {
            const msg = err instanceof Error ? err.message : 'Unknown error';
            res.status(400).json({ error: msg });
        }
    });
    // ======================== INDEXER ========================
    app.get('/api/v1/blocks', async (req, res) => {
        const limit = Math.min(parseInt(String(req.query.limit) || '20'), 100);
        const result = await connection_1.db.query(`SELECT * FROM indexed_blocks WHERE status = 'confirmed' ORDER BY block_number DESC LIMIT $1`, [limit]);
        res.json({ blocks: result.rows });
    });
    app.get('/api/v1/blocks/:number', async (req, res) => {
        const result = await connection_1.db.query(`SELECT * FROM indexed_blocks WHERE block_number = $1 AND status = 'confirmed'`, [param(req, 'number')]);
        if (result.rows.length === 0)
            return res.status(404).json({ error: 'Block not found' });
        res.json(result.rows[0]);
    });
    app.get('/api/v1/events', async (req, res) => {
        const contract = req.query.contract ? String(req.query.contract) : undefined;
        const event = req.query.event ? String(req.query.event) : undefined;
        const limit = Math.min(parseInt(String(req.query.limit) || '50'), 200);
        let query = 'SELECT * FROM indexed_events WHERE 1=1';
        const params = [];
        if (contract) {
            params.push(contract);
            query += ` AND contract_address = $${params.length}`;
        }
        if (event) {
            params.push(event);
            query += ` AND event_signature = $${params.length}`;
        }
        query += ` ORDER BY id DESC LIMIT $${params.length + 1}`;
        params.push(limit);
        const result = await connection_1.db.query(query, params);
        res.json({ events: result.rows });
    });
    // ======================== RECONCILIATION ========================
    app.get('/api/v1/reconciliation/runs', async (_req, res) => {
        const result = await connection_1.db.query('SELECT * FROM reconciliation_runs ORDER BY started_at DESC LIMIT 20');
        res.json({ runs: result.rows });
    });
    // Error handler
    app.use((err, _req, res, _next) => {
        config_1.logger.error(err, 'Unhandled API error');
        res.status(500).json({ error: 'Internal server error' });
    });
    return app;
}
//# sourceMappingURL=app.js.map