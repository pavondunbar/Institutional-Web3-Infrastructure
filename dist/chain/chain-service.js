"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ChainService = void 0;
const ethers_1 = require("ethers");
const connection_1 = require("../database/connection");
const redis_1 = require("../cache/redis");
const config_1 = require("../config");
const wallet_service_1 = require("../wallet/wallet-service");
/**
 * ChainService: Manages the full transaction lifecycle for Ethereum L1.
 * Handles submission, confirmation tracking, finality, gas estimation,
 * stuck transaction detection, and resubmission.
 */
class ChainService {
    provider;
    walletService = new wallet_service_1.WalletService();
    running = false;
    pollTimer = null;
    chain;
    pollMs = 5000;
    constructor() {
        this.provider = new ethers_1.JsonRpcProvider(config_1.config.ethereum.rpcUrl);
        this.chain = `ethereum:${config_1.config.ethereum.chainId}`;
    }
    async start() {
        this.running = true;
        this.monitorPendingTxs();
        config_1.logger.info({ chain: this.chain }, 'Chain service started');
    }
    async stop() {
        this.running = false;
        if (this.pollTimer)
            clearTimeout(this.pollTimer);
    }
    /**
     * Submit a signed transaction to the network.
     */
    async submitTransaction(txId, signedTx) {
        try {
            const response = await this.provider.broadcastTransaction(signedTx);
            await this.walletService.markSubmitted(txId, response.hash);
            await redis_1.txStatusCache.set(response.hash, 'submitted');
            config_1.logger.info({ txId, txHash: response.hash }, 'Transaction submitted');
            return response.hash;
        }
        catch (err) {
            const message = err instanceof Error ? err.message : 'Unknown submission error';
            await this.walletService.markFailed(txId, message);
            throw err;
        }
    }
    /**
     * Estimate gas for a transaction.
     */
    async estimateGas(to, value, data) {
        const [gasLimit, feeData] = await Promise.all([
            this.provider.estimateGas({ to, value, data }),
            this.provider.getFeeData(),
        ]);
        return {
            gasLimit,
            gasPrice: feeData.gasPrice || 0n,
            maxFeePerGas: feeData.maxFeePerGas || 0n,
            maxPriorityFeePerGas: feeData.maxPriorityFeePerGas || 0n,
        };
    }
    /**
     * Get current chain state.
     */
    async getChainState() {
        const [blockNumber, feeData] = await Promise.all([
            this.provider.getBlockNumber(),
            this.provider.getFeeData(),
        ]);
        return {
            chain: this.chain,
            blockNumber,
            gasPrice: feeData.gasPrice?.toString(),
            maxFeePerGas: feeData.maxFeePerGas?.toString(),
            baseFee: feeData.maxFeePerGas?.toString(),
        };
    }
    /**
     * Get transaction receipt with finality check.
     */
    async getTransactionStatus(txHash) {
        const receipt = await this.provider.getTransactionReceipt(txHash);
        if (!receipt)
            return { status: 'pending', confirmations: 0 };
        const currentBlock = await this.provider.getBlockNumber();
        const confirmations = currentBlock - receipt.blockNumber + 1;
        const finalized = confirmations >= config_1.config.ethereum.confirmations;
        return {
            status: receipt.status === 1 ? (finalized ? 'finalized' : 'confirmed') : 'reverted',
            confirmations,
            blockNumber: receipt.blockNumber,
            blockHash: receipt.blockHash,
            gasUsed: receipt.gasUsed,
            finalized,
        };
    }
    /**
     * Monitor pending/submitted transactions and update their status.
     */
    monitorPendingTxs() {
        if (!this.running)
            return;
        this.checkSubmittedTransactions()
            .catch(err => config_1.logger.error(err, 'Tx monitor error'))
            .finally(() => {
            if (this.running) {
                this.pollTimer = setTimeout(() => this.monitorPendingTxs(), this.pollMs);
            }
        });
    }
    async checkSubmittedTransactions() {
        const result = await connection_1.db.query(`SELECT id, tx_hash, submitted_at FROM transactions_blockchain
       WHERE chain = $1 AND status = 'submitted' AND tx_hash IS NOT NULL
       ORDER BY submitted_at ASC LIMIT 50`, [this.chain]);
        for (const tx of result.rows) {
            try {
                const status = await this.getTransactionStatus(tx.tx_hash);
                if (status.status === 'finalized') {
                    await this.walletService.markConfirmed(tx.id, status.blockNumber, status.blockHash, status.gasUsed, status.confirmations);
                    await redis_1.txStatusCache.set(tx.tx_hash, 'confirmed');
                }
                else if (status.status === 'reverted') {
                    await this.walletService.markFailed(tx.id, 'Transaction reverted on-chain');
                    await redis_1.txStatusCache.set(tx.tx_hash, 'failed');
                }
                else {
                    // Check for stuck transactions (no confirmation after threshold)
                    const age = Date.now() - new Date(tx.submitted_at).getTime();
                    if (age > 15 * 60 * 1000) { // 15 minutes
                        config_1.logger.warn({ txId: tx.id, txHash: tx.tx_hash, ageMs: age }, 'Transaction may be stuck');
                        await connection_1.db.query(`INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
               VALUES ('transaction', $1, 'tx.stuck', $2)`, [tx.id, JSON.stringify({ txId: tx.id, txHash: tx.tx_hash, ageMs: age })]);
                    }
                }
            }
            catch (err) {
                config_1.logger.error({ txId: tx.id, err }, 'Error checking tx status');
            }
        }
    }
}
exports.ChainService = ChainService;
//# sourceMappingURL=chain-service.js.map