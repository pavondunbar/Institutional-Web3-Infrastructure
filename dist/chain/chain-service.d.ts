/**
 * ChainService: Manages the full transaction lifecycle for Ethereum L1.
 * Handles submission, confirmation tracking, finality, gas estimation,
 * stuck transaction detection, and resubmission.
 */
export declare class ChainService {
    private provider;
    private walletService;
    private running;
    private pollTimer;
    private readonly chain;
    private readonly pollMs;
    constructor();
    start(): Promise<void>;
    stop(): Promise<void>;
    /**
     * Submit a signed transaction to the network.
     */
    submitTransaction(txId: string, signedTx: string): Promise<string>;
    /**
     * Estimate gas for a transaction.
     */
    estimateGas(to: string, value: bigint, data?: string): Promise<{
        gasLimit: bigint;
        gasPrice: bigint;
        maxFeePerGas: bigint;
        maxPriorityFeePerGas: bigint;
    }>;
    /**
     * Get current chain state.
     */
    getChainState(): Promise<{
        chain: string;
        blockNumber: number;
        gasPrice: string | undefined;
        maxFeePerGas: string | undefined;
        baseFee: string | undefined;
    }>;
    /**
     * Get transaction receipt with finality check.
     */
    getTransactionStatus(txHash: string): Promise<{
        status: string;
        confirmations: number;
        blockNumber?: undefined;
        blockHash?: undefined;
        gasUsed?: undefined;
        finalized?: undefined;
    } | {
        status: string;
        confirmations: number;
        blockNumber: number;
        blockHash: string;
        gasUsed: bigint;
        finalized: boolean;
    }>;
    /**
     * Monitor pending/submitted transactions and update their status.
     */
    private monitorPendingTxs;
    private checkSubmittedTransactions;
}
//# sourceMappingURL=chain-service.d.ts.map