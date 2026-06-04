export interface CreateWalletRequest {
    accountId: string;
    chain: string;
    address: string;
    walletType: 'hot' | 'warm' | 'cold' | 'deposit';
    keyId?: string;
    metadata?: Record<string, unknown>;
}
export interface SubmitTxRequest {
    walletId: string;
    toAddress: string;
    amount: bigint;
    gasLimit?: bigint;
    gasPrice?: bigint;
    metadata?: Record<string, unknown>;
}
export declare class WalletService {
    /**
     * Register a wallet in the system.
     */
    createWallet(req: CreateWalletRequest): Promise<string>;
    /**
     * Get wallet by ID
     */
    getWallet(walletId: string): Promise<any>;
    /**
     * Get wallets for an account
     */
    getWalletsByAccount(accountId: string): Promise<any[]>;
    /**
     * Create and queue a blockchain transaction for signing.
     * Pipeline: create → sign → submit → monitor
     */
    createTransaction(req: SubmitTxRequest): Promise<string>;
    /**
     * Mark transaction as signed and submitted to chain.
     * Called by the signing service after KMS/HSM signing completes.
     */
    markSubmitted(txId: string, txHash: string): Promise<void>;
    /**
     * Mark transaction as confirmed with block info.
     */
    markConfirmed(txId: string, blockNumber: number, blockHash: string, gasUsed: bigint, confirmations: number): Promise<void>;
    /**
     * Mark transaction as failed.
     */
    markFailed(txId: string, error: string): Promise<void>;
    /**
     * Handle reorg: mark affected transactions.
     */
    handleReorg(blockNumber: number, chain: string): Promise<string[]>;
}
/**
 * Signing Service Interface - separates protocol logic from security controls.
 * In production, this would integrate with MPC/HSM/KMS systems.
 */
export interface SigningService {
    sign(params: {
        keyId: string;
        chain: string;
        txData: {
            to: string;
            value: string;
            nonce: number;
            gasLimit: string;
            gasPrice: string;
            chainId: number;
        };
    }): Promise<string>;
}
/**
 * Local signer for development/testing only.
 * Production must use HSM/MPC signing service.
 */
export declare class LocalDevSigner implements SigningService {
    private signers;
    addKey(keyId: string, privateKey: string): void;
    sign(params: {
        keyId: string;
        chain: string;
        txData: {
            to: string;
            value: string;
            nonce: number;
            gasLimit: string;
            gasPrice: string;
            chainId: number;
        };
    }): Promise<string>;
}
//# sourceMappingURL=wallet-service.d.ts.map