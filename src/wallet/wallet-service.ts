import { v4 as uuidv4 } from 'uuid';
import { db } from '../database/connection';
import { nonceManager } from '../cache/redis';
import { logger } from '../config';

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

export class WalletService {
  /**
   * Register a wallet in the system.
   */
  async createWallet(req: CreateWalletRequest): Promise<string> {
    const result = await db.query(
      `INSERT INTO wallets (id, account_id, chain, address, wallet_type, key_id, metadata)
       VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
      [uuidv4(), req.accountId, req.chain, req.address, req.walletType, req.keyId || null, JSON.stringify(req.metadata || {})]
    );
    return result.rows[0].id;
  }

  /**
   * Get wallet by ID
   */
  async getWallet(walletId: string) {
    const result = await db.query('SELECT * FROM wallets WHERE id = $1', [walletId]);
    return result.rows[0] || null;
  }

  /**
   * Get wallets for an account
   */
  async getWalletsByAccount(accountId: string) {
    const result = await db.query('SELECT * FROM wallets WHERE account_id = $1 AND status = $2', [accountId, 'active']);
    return result.rows;
  }

  /**
   * Create and queue a blockchain transaction for signing.
   * Pipeline: create → sign → submit → monitor
   */
  async createTransaction(req: SubmitTxRequest): Promise<string> {
    const wallet = await this.getWallet(req.walletId);
    if (!wallet) throw new Error('Wallet not found');
    if (wallet.status !== 'active') throw new Error('Wallet is not active');

    // Get next nonce atomically from Redis
    const nonce = await nonceManager.getAndIncrement(wallet.chain, wallet.address);

    const txId = uuidv4();
    await db.query(
      `INSERT INTO transactions_blockchain
       (id, wallet_id, chain, from_address, to_address, amount, gas_limit, gas_price, nonce, status, metadata)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'pending', $10)`,
      [txId, req.walletId, wallet.chain, wallet.address, req.toAddress,
       req.amount.toString(), (req.gasLimit || 21000n).toString(), (req.gasPrice || 0n).toString(),
       nonce, JSON.stringify(req.metadata || {})]
    );

    // Write outbox event for signing service to pick up
    await db.query(
      `INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
       VALUES ('transaction', $1, 'tx.created', $2)`,
      [txId, JSON.stringify({
        txId, walletId: req.walletId, chain: wallet.chain,
        from: wallet.address, to: req.toAddress,
        amount: req.amount.toString(), nonce, gasLimit: (req.gasLimit || 21000n).toString(),
        keyId: wallet.key_id,
      })]
    );

    logger.info({ txId, nonce }, 'Transaction created and queued for signing');
    return txId;
  }

  /**
   * Mark transaction as signed and submitted to chain.
   * Called by the signing service after KMS/HSM signing completes.
   */
  async markSubmitted(txId: string, txHash: string): Promise<void> {
    await db.query(
      `UPDATE transactions_blockchain
       SET status = 'submitted', tx_hash = $1, submitted_at = NOW(), updated_at = NOW()
       WHERE id = $2 AND status = 'signing'`,
      [txHash, txId]
    );

    await db.query(
      `INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
       VALUES ('transaction', $1, 'tx.submitted', $2)`,
      [txId, JSON.stringify({ txId, txHash })]
    );
  }

  /**
   * Mark transaction as confirmed with block info.
   */
  async markConfirmed(txId: string, blockNumber: number, blockHash: string, gasUsed: bigint, confirmations: number): Promise<void> {
    await db.query(
      `UPDATE transactions_blockchain
       SET status = 'confirmed', block_number = $1, block_hash = $2, gas_used = $3,
           confirmations = $4, confirmed_at = NOW(), updated_at = NOW()
       WHERE id = $5`,
      [blockNumber, blockHash, gasUsed.toString(), confirmations, txId]
    );

    await db.query(
      `INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
       VALUES ('transaction', $1, 'tx.confirmed', $2)`,
      [txId, JSON.stringify({ txId, blockNumber, blockHash, confirmations })]
    );
  }

  /**
   * Mark transaction as failed.
   */
  async markFailed(txId: string, error: string): Promise<void> {
    await db.query(
      `UPDATE transactions_blockchain
       SET status = 'failed', error_message = $1, updated_at = NOW()
       WHERE id = $2`,
      [error, txId]
    );

    await db.query(
      `INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
       VALUES ('transaction', $1, 'tx.failed', $2)`,
      [txId, JSON.stringify({ txId, error })]
    );
  }

  /**
   * Handle reorg: mark affected transactions.
   */
  async handleReorg(blockNumber: number, chain: string): Promise<string[]> {
    const result = await db.query(
      `UPDATE transactions_blockchain
       SET status = 'reorged', updated_at = NOW()
       WHERE block_number >= $1 AND chain = $2 AND status = 'confirmed'
       RETURNING id`,
      [blockNumber, chain]
    );
    const txIds = result.rows.map(r => r.id);
    logger.warn({ blockNumber, chain, count: txIds.length }, 'Transactions marked as reorged');
    return txIds;
  }
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
  }): Promise<string>; // returns signed tx hex
}

/**
 * Local signer for development/testing only.
 * Production must use HSM/MPC signing service.
 */
export class LocalDevSigner implements SigningService {
  private signers: Map<string, import('ethers').Wallet> = new Map();

  addKey(keyId: string, privateKey: string) {
    const { Wallet } = require('ethers') as typeof import('ethers');
    this.signers.set(keyId, new Wallet(privateKey));
  }

  async sign(params: { keyId: string; chain: string; txData: { to: string; value: string; nonce: number; gasLimit: string; gasPrice: string; chainId: number } }): Promise<string> {
    const signer = this.signers.get(params.keyId);
    if (!signer) throw new Error(`Key ${params.keyId} not found`);

    const signedTx = await signer.signTransaction({
      to: params.txData.to,
      value: BigInt(params.txData.value),
      nonce: params.txData.nonce,
      gasLimit: BigInt(params.txData.gasLimit),
      gasPrice: BigInt(params.txData.gasPrice),
      chainId: params.txData.chainId,
    });

    return signedTx;
  }
}
