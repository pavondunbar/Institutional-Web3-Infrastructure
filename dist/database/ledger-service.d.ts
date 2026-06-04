import { TxClient } from './connection';
export interface LedgerLine {
    accountId: string;
    amount: bigint;
    direction: 'debit' | 'credit';
}
export interface PostJournalRequest {
    idempotencyKey: string;
    description?: string;
    externalRef?: string;
    externalRefType?: string;
    lines: LedgerLine[];
    metadata?: Record<string, unknown>;
}
export interface PostedJournal {
    journalEntryId: string;
    entries: {
        id: string;
        accountId: string;
        amount: bigint;
        direction: string;
    }[];
}
/**
 * Post a double-entry journal to the append-only ledger.
 * Runs in SERIALIZABLE isolation. All-or-nothing atomic posting.
 * Hash chain, balance cache, and outbox event in a single transaction.
 */
export declare function postJournal(req: PostJournalRequest): Promise<PostedJournal>;
/**
 * Reverse a journal entry by posting a mirror entry.
 * Original entry is marked as 'reversed'.
 */
export declare function reverseJournal(journalEntryId: string, idempotencyKey: string): Promise<PostedJournal>;
/**
 * Get balance for an account from cache.
 */
export declare function getBalance(accountId: string, client?: TxClient): Promise<{
    accountId: any;
    debitTotal: bigint;
    creditTotal: bigint;
    balance: bigint;
    lastEntryHash: any;
} | null>;
/**
 * Reconstruct balance from ledger entries (for reconciliation).
 */
export declare function reconstructBalance(accountId: string): Promise<{
    debitTotal: bigint;
    creditTotal: bigint;
    balance: bigint;
}>;
//# sourceMappingURL=ledger-service.d.ts.map