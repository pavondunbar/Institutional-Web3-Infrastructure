/**
 * BlockIndexer: Ingests blocks from Ethereum-compatible chains.
 * Handles reorgs, indexes events, and triggers downstream processing.
 */
export declare class BlockIndexer {
    private provider;
    private running;
    private pollTimer;
    private readonly chain;
    private readonly confirmations;
    private readonly pollMs;
    private walletService;
    constructor();
    start(): Promise<void>;
    stop(): Promise<void>;
    private poll;
    private indexNext;
    private detectReorg;
    private handleReorg;
    private processBlock;
    private indexEvents;
}
/**
 * EventProcessor: Processes indexed events and triggers business logic.
 * Runs as a separate process to decouple indexing from processing.
 */
export declare class EventProcessor {
    private running;
    private pollTimer;
    private readonly batchSize;
    private readonly pollMs;
    private handlers;
    registerHandler(eventSignature: string, handler: (event: IndexedEvent) => Promise<void>): void;
    start(): Promise<void>;
    stop(): void;
    private poll;
    private processBatch;
}
export interface IndexedEvent {
    id: number;
    chain: string;
    txHash: string;
    logIndex: number;
    contractAddress: string;
    eventSignature: string;
    topics: string[];
    data: Buffer;
    blockNumber: number;
    blockHash: string;
}
//# sourceMappingURL=block-indexer.d.ts.map