"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.postJournal = postJournal;
exports.reverseJournal = reverseJournal;
exports.getBalance = getBalance;
exports.reconstructBalance = reconstructBalance;
const crypto_1 = require("crypto");
const connection_1 = require("./connection");
const config_1 = require("../config");
function computeEntryHash(prevHash, accountId, amount, direction, seq) {
    const data = `${prevHash}|${accountId}|${amount}|${direction}|${seq}`;
    return (0, crypto_1.createHash)('sha256').update(data).digest('hex');
}
/**
 * Post a double-entry journal to the append-only ledger.
 * Runs in SERIALIZABLE isolation. All-or-nothing atomic posting.
 * Hash chain, balance cache, and outbox event in a single transaction.
 */
async function postJournal(req) {
    // Validate balanced before hitting DB
    const debitSum = req.lines.filter(l => l.direction === 'debit').reduce((s, l) => s + l.amount, 0n);
    const creditSum = req.lines.filter(l => l.direction === 'credit').reduce((s, l) => s + l.amount, 0n);
    if (debitSum !== creditSum) {
        throw new Error(`Unbalanced journal: debits=${debitSum} credits=${creditSum}`);
    }
    if (req.lines.length === 0) {
        throw new Error('Journal must have at least one line');
    }
    return (0, connection_1.withSerializableTransaction)(async (client) => {
        // Set deferred constraints so balanced check runs at COMMIT
        await client.query('SET CONSTRAINTS trg_check_balanced DEFERRED');
        // Idempotency check
        const existing = await client.query('SELECT id FROM journal_entries WHERE idempotency_key = $1', [req.idempotencyKey]);
        if (existing.rows.length > 0) {
            config_1.logger.info({ key: req.idempotencyKey }, 'Duplicate journal entry, returning existing');
            const entries = await client.query('SELECT id, account_id, amount, direction FROM ledger_entries WHERE journal_entry_id = $1', [existing.rows[0].id]);
            return {
                journalEntryId: existing.rows[0].id,
                entries: entries.rows.map(r => ({
                    id: r.id, accountId: r.account_id, amount: BigInt(r.amount), direction: r.direction
                })),
            };
        }
        // Create journal entry
        const journalResult = await client.query(`INSERT INTO journal_entries (idempotency_key, description, external_ref, external_ref_type, metadata)
       VALUES ($1, $2, $3, $4, $5) RETURNING id`, [req.idempotencyKey, req.description || null, req.externalRef || null, req.externalRefType || null, JSON.stringify(req.metadata || {})]);
        const journalId = journalResult.rows[0].id;
        const postedEntries = [];
        for (const line of req.lines) {
            // Lock the balance row for this account (or create it)
            let balanceRow = await client.query('SELECT * FROM balance_cache WHERE account_id = $1 FOR UPDATE', [line.accountId]);
            if (balanceRow.rows.length === 0) {
                await client.query('INSERT INTO balance_cache (account_id) VALUES ($1) ON CONFLICT DO NOTHING', [line.accountId]);
                balanceRow = await client.query('SELECT * FROM balance_cache WHERE account_id = $1 FOR UPDATE', [line.accountId]);
            }
            const bal = balanceRow.rows[0];
            const prevHash = bal.last_entry_hash || '0000000000000000000000000000000000000000000000000000000000000000';
            const seqNum = bal.last_entry_id ? Number(bal.last_entry_id) + 1 : 1;
            // Get actual sequence from ledger
            const seqResult = await client.query('SELECT COALESCE(MAX(sequence_num), 0) as max_seq FROM ledger_entries WHERE account_id = $1', [line.accountId]);
            const nextSeq = Number(seqResult.rows[0].max_seq) + 1;
            const entryHash = computeEntryHash(prevHash, line.accountId, line.amount, line.direction, nextSeq);
            // Insert ledger entry
            const entryResult = await client.query(`INSERT INTO ledger_entries (journal_entry_id, account_id, amount, direction, entry_hash, prev_hash, sequence_num)
         VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`, [journalId, line.accountId, line.amount.toString(), line.direction, entryHash, prevHash, nextSeq]);
            // Update balance cache
            const newDebitTotal = BigInt(bal.debit_total) + (line.direction === 'debit' ? line.amount : 0n);
            const newCreditTotal = BigInt(bal.credit_total) + (line.direction === 'credit' ? line.amount : 0n);
            const newBalance = newDebitTotal - newCreditTotal;
            await client.query(`UPDATE balance_cache SET debit_total = $1, credit_total = $2, balance = $3,
         last_entry_id = $4, last_entry_hash = $5, updated_at = NOW()
         WHERE account_id = $6`, [newDebitTotal.toString(), newCreditTotal.toString(), newBalance.toString(),
                entryResult.rows[0].id, entryHash, line.accountId]);
            postedEntries.push({
                id: entryResult.rows[0].id,
                accountId: line.accountId,
                amount: line.amount,
                direction: line.direction,
            });
        }
        // Write to transactional outbox (same transaction = atomic)
        await client.query(`INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
       VALUES ('journal_entry', $1, 'journal.posted', $2)`, [journalId, JSON.stringify({
                journalEntryId: journalId,
                idempotencyKey: req.idempotencyKey,
                lines: postedEntries.map(e => ({
                    accountId: e.accountId,
                    amount: e.amount.toString(),
                    direction: e.direction,
                })),
            })]);
        return { journalEntryId: journalId, entries: postedEntries };
    });
}
/**
 * Reverse a journal entry by posting a mirror entry.
 * Original entry is marked as 'reversed'.
 */
async function reverseJournal(journalEntryId, idempotencyKey) {
    return (0, connection_1.withSerializableTransaction)(async (client) => {
        const original = await client.query('SELECT * FROM journal_entries WHERE id = $1', [journalEntryId]);
        if (original.rows.length === 0)
            throw new Error('Journal entry not found');
        if (original.rows[0].status === 'reversed')
            throw new Error('Journal entry already reversed');
        const originalLines = await client.query('SELECT account_id, amount, direction FROM ledger_entries WHERE journal_entry_id = $1', [journalEntryId]);
        // Post reversal (swap directions)
        const reversalLines = originalLines.rows.map(r => ({
            accountId: r.account_id,
            amount: BigInt(r.amount),
            direction: r.direction === 'debit' ? 'credit' : 'debit',
        }));
        // Mark original as reversed
        await client.query('UPDATE journal_entries SET status = $1, reversed_by = NULL WHERE id = $2', ['reversed', journalEntryId]);
        // Release the client so postJournal can get its own
        // Actually, we need to do the reversal within this same tx
        // So we inline the posting logic
        await client.query('SET CONSTRAINTS trg_check_balanced DEFERRED');
        const reversalResult = await client.query(`INSERT INTO journal_entries (idempotency_key, description, external_ref, external_ref_type, metadata)
       VALUES ($1, $2, $3, $4, $5) RETURNING id`, [idempotencyKey, `Reversal of ${journalEntryId}`, journalEntryId, 'reversal', '{}']);
        const reversalId = reversalResult.rows[0].id;
        // Update the original to point to reversal
        await client.query('UPDATE journal_entries SET reversed_by = $1 WHERE id = $2', [reversalId, journalEntryId]);
        const postedEntries = [];
        for (const line of reversalLines) {
            const balanceRow = await client.query('SELECT * FROM balance_cache WHERE account_id = $1 FOR UPDATE', [line.accountId]);
            const bal = balanceRow.rows[0];
            const prevHash = bal.last_entry_hash || '0000000000000000000000000000000000000000000000000000000000000000';
            const seqResult = await client.query('SELECT COALESCE(MAX(sequence_num), 0) as max_seq FROM ledger_entries WHERE account_id = $1', [line.accountId]);
            const nextSeq = Number(seqResult.rows[0].max_seq) + 1;
            const entryHash = computeEntryHash(prevHash, line.accountId, line.amount, line.direction, nextSeq);
            const entryResult = await client.query(`INSERT INTO ledger_entries (journal_entry_id, account_id, amount, direction, entry_hash, prev_hash, sequence_num)
         VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`, [reversalId, line.accountId, line.amount.toString(), line.direction, entryHash, prevHash, nextSeq]);
            const newDebitTotal = BigInt(bal.debit_total) + (line.direction === 'debit' ? line.amount : 0n);
            const newCreditTotal = BigInt(bal.credit_total) + (line.direction === 'credit' ? line.amount : 0n);
            const newBalance = newDebitTotal - newCreditTotal;
            await client.query(`UPDATE balance_cache SET debit_total = $1, credit_total = $2, balance = $3,
         last_entry_id = $4, last_entry_hash = $5, updated_at = NOW()
         WHERE account_id = $6`, [newDebitTotal.toString(), newCreditTotal.toString(), newBalance.toString(),
                entryResult.rows[0].id, entryHash, line.accountId]);
            postedEntries.push({ id: entryResult.rows[0].id, accountId: line.accountId, amount: line.amount, direction: line.direction });
        }
        // Outbox event
        await client.query(`INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
       VALUES ('journal_entry', $1, 'journal.reversed', $2)`, [reversalId, JSON.stringify({ originalId: journalEntryId, reversalId })]);
        return { journalEntryId: reversalId, entries: postedEntries };
    });
}
/**
 * Get balance for an account from cache.
 */
async function getBalance(accountId, client) {
    const q = client || (await Promise.resolve().then(() => __importStar(require('./connection')))).db;
    const result = await q.query('SELECT * FROM balance_cache WHERE account_id = $1', [accountId]);
    if (result.rows.length === 0)
        return null;
    const row = result.rows[0];
    return {
        accountId: row.account_id,
        debitTotal: BigInt(row.debit_total),
        creditTotal: BigInt(row.credit_total),
        balance: BigInt(row.balance),
        lastEntryHash: row.last_entry_hash,
    };
}
/**
 * Reconstruct balance from ledger entries (for reconciliation).
 */
async function reconstructBalance(accountId) {
    const { db: pool } = await Promise.resolve().then(() => __importStar(require('./connection')));
    const result = await pool.query(`SELECT
       COALESCE(SUM(CASE WHEN direction = 'debit' THEN amount ELSE 0 END), 0) as debit_total,
       COALESCE(SUM(CASE WHEN direction = 'credit' THEN amount ELSE 0 END), 0) as credit_total
     FROM ledger_entries WHERE account_id = $1`, [accountId]);
    const row = result.rows[0];
    return {
        debitTotal: BigInt(row.debit_total),
        creditTotal: BigInt(row.credit_total),
        balance: BigInt(row.debit_total) - BigInt(row.credit_total),
    };
}
//# sourceMappingURL=ledger-service.js.map