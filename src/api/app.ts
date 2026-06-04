import express, { Request, Response, NextFunction, RequestHandler } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { postJournal, reverseJournal, getBalance } from '../database/ledger-service';
import { WalletService } from '../wallet/wallet-service';
import { ChainService } from '../chain/chain-service';
import { db } from '../database/connection';
import { rateLimiter } from '../cache/redis';
import { logger } from '../config';

function param(req: Request, name: string): string {
  const val = req.params[name];
  return Array.isArray(val) ? val[0] : val;
}

export function createApp() {
  const app = express();
  app.use(express.json());

  const walletService = new WalletService();
  const chainService = new ChainService();

  // Rate limiting middleware
  app.use(async (req: Request, res: Response, next: NextFunction) => {
    const key = (Array.isArray(req.ip) ? req.ip[0] : req.ip) || 'unknown';
    const allowed = await rateLimiter.check(key, 100, 60);
    if (!allowed) return res.status(429).json({ error: 'Rate limit exceeded' });
    next();
  });

  // ======================== HEALTH ========================
  app.get('/health', (_req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
  });

  // ======================== LEDGER ========================
  app.post('/api/v1/journal', async (req: Request, res: Response) => {
    try {
      const { idempotencyKey, description, externalRef, externalRefType, lines, metadata } = req.body;
      if (!idempotencyKey || !lines || lines.length === 0) {
        return res.status(400).json({ error: 'idempotencyKey and lines are required' });
      }
      const result = await postJournal({
        idempotencyKey,
        description,
        externalRef,
        externalRefType,
        lines: lines.map((l: { accountId: string; amount: string; direction: string }) => ({
          accountId: l.accountId,
          amount: BigInt(l.amount),
          direction: l.direction as 'debit' | 'credit',
        })),
        metadata,
      });
      res.status(201).json({
        journalEntryId: result.journalEntryId,
        entries: result.entries.map(e => ({ ...e, amount: e.amount.toString() })),
      });
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : 'Unknown error';
      logger.error(err, 'Journal posting failed');
      res.status(400).json({ error: msg });
    }
  });

  app.post('/api/v1/journal/:id/reverse', async (req: Request, res: Response) => {
    try {
      const idempotencyKey = req.body.idempotencyKey || uuidv4();
      const result = await reverseJournal(param(req, 'id'), idempotencyKey);
      res.json({ journalEntryId: result.journalEntryId, entries: result.entries.map(e => ({ ...e, amount: e.amount.toString() })) });
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : 'Unknown error';
      res.status(400).json({ error: msg });
    }
  });

  app.get('/api/v1/accounts/:id/balance', async (req: Request, res: Response) => {
    try {
      const balance = await getBalance(param(req, 'id'));
      if (!balance) return res.status(404).json({ error: 'Account not found' });
      res.json({
        accountId: balance.accountId,
        debitTotal: balance.debitTotal.toString(),
        creditTotal: balance.creditTotal.toString(),
        balance: balance.balance.toString(),
        lastEntryHash: balance.lastEntryHash,
      });
    } catch (err: unknown) {
      res.status(500).json({ error: 'Internal error' });
    }
  });

  app.get('/api/v1/accounts/:id/history', async (req: Request, res: Response) => {
    try {
      const limit = Math.min(parseInt(String(req.query.limit) || '50'), 200);
      const offset = parseInt(String(req.query.offset) || '0');
      const result = await db.query(
        `SELECT l.*, j.description, j.external_ref, j.status as journal_status
         FROM ledger_entries l JOIN journal_entries j ON l.journal_entry_id = j.id
         WHERE l.account_id = $1 ORDER BY l.sequence_num DESC LIMIT $2 OFFSET $3`,
        [param(req, 'id'), limit, offset]
      );
      res.json({ entries: result.rows, limit, offset });
    } catch (err: unknown) {
      res.status(500).json({ error: 'Internal error' });
    }
  });

  // ======================== WALLETS ========================
  app.post('/api/v1/wallets', async (req: Request, res: Response) => {
    try {
      const id = await walletService.createWallet(req.body);
      res.status(201).json({ walletId: id });
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : 'Unknown error';
      res.status(400).json({ error: msg });
    }
  });

  app.get('/api/v1/wallets/:id', async (req: Request, res: Response) => {
    const wallet = await walletService.getWallet(param(req, 'id'));
    if (!wallet) return res.status(404).json({ error: 'Wallet not found' });
    res.json(wallet);
  });

  app.post('/api/v1/wallets/:id/transactions', async (req: Request, res: Response) => {
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
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : 'Unknown error';
      res.status(400).json({ error: msg });
    }
  });

  // ======================== CHAIN ========================
  app.get('/api/v1/chain/state', async (_req: Request, res: Response) => {
    try {
      const state = await chainService.getChainState();
      res.json(state);
    } catch (err: unknown) {
      res.status(500).json({ error: 'Failed to get chain state' });
    }
  });

  app.get('/api/v1/chain/tx/:hash', async (req: Request, res: Response) => {
    try {
      const status = await chainService.getTransactionStatus(param(req, 'hash'));
      res.json(status);
    } catch (err: unknown) {
      res.status(500).json({ error: 'Failed to get tx status' });
    }
  });

  app.post('/api/v1/chain/estimate-gas', async (req: Request, res: Response) => {
    try {
      const estimate = await chainService.estimateGas(
        req.body.to, BigInt(req.body.value || '0'), req.body.data
      );
      res.json({
        gasLimit: estimate.gasLimit.toString(),
        gasPrice: estimate.gasPrice.toString(),
        maxFeePerGas: estimate.maxFeePerGas.toString(),
        maxPriorityFeePerGas: estimate.maxPriorityFeePerGas.toString(),
      });
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : 'Unknown error';
      res.status(400).json({ error: msg });
    }
  });

  // ======================== INDEXER ========================
  app.get('/api/v1/blocks', async (req: Request, res: Response) => {
    const limit = Math.min(parseInt(String(req.query.limit) || '20'), 100);
    const result = await db.query(
      `SELECT * FROM indexed_blocks WHERE status = 'confirmed' ORDER BY block_number DESC LIMIT $1`,
      [limit]
    );
    res.json({ blocks: result.rows });
  });

  app.get('/api/v1/blocks/:number', async (req: Request, res: Response) => {
    const result = await db.query(
      `SELECT * FROM indexed_blocks WHERE block_number = $1 AND status = 'confirmed'`,
      [param(req, 'number')]
    );
    if (result.rows.length === 0) return res.status(404).json({ error: 'Block not found' });
    res.json(result.rows[0]);
  });

  app.get('/api/v1/events', async (req: Request, res: Response) => {
    const contract = req.query.contract ? String(req.query.contract) : undefined;
    const event = req.query.event ? String(req.query.event) : undefined;
    const limit = Math.min(parseInt(String(req.query.limit) || '50'), 200);
    let query = 'SELECT * FROM indexed_events WHERE 1=1';
    const params: unknown[] = [];
    if (contract) { params.push(contract); query += ` AND contract_address = $${params.length}`; }
    if (event) { params.push(event); query += ` AND event_signature = $${params.length}`; }
    query += ` ORDER BY id DESC LIMIT $${params.length + 1}`;
    params.push(limit);
    const result = await db.query(query, params);
    res.json({ events: result.rows });
  });

  // ======================== RECONCILIATION ========================
  app.get('/api/v1/reconciliation/runs', async (_req: Request, res: Response) => {
    const result = await db.query('SELECT * FROM reconciliation_runs ORDER BY started_at DESC LIMIT 20');
    res.json({ runs: result.rows });
  });

  // Error handler
  app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
    logger.error(err, 'Unhandled API error');
    res.status(500).json({ error: 'Internal server error' });
  });

  return app;
}
