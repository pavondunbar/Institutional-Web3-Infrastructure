import { Pool, PoolClient } from 'pg';
export declare const db: Pool;
export type TxClient = PoolClient;
/**
 * Execute a callback within a SERIALIZABLE transaction with proper cleanup.
 */
export declare function withSerializableTransaction<T>(fn: (client: TxClient) => Promise<T>): Promise<T>;
//# sourceMappingURL=connection.d.ts.map