"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.db = void 0;
exports.withSerializableTransaction = withSerializableTransaction;
const pg_1 = require("pg");
const config_1 = require("../config");
exports.db = new pg_1.Pool({
    host: config_1.config.postgres.host,
    port: config_1.config.postgres.port,
    database: config_1.config.postgres.database,
    user: config_1.config.postgres.user,
    password: config_1.config.postgres.password,
    max: config_1.config.postgres.max,
});
exports.db.on('error', (err) => {
    config_1.logger.error(err, 'Unexpected database pool error');
});
/**
 * Execute a callback within a SERIALIZABLE transaction with proper cleanup.
 */
async function withSerializableTransaction(fn) {
    const client = await exports.db.connect();
    try {
        await client.query('BEGIN ISOLATION LEVEL SERIALIZABLE');
        const result = await fn(client);
        await client.query('COMMIT');
        return result;
    }
    catch (err) {
        await client.query('ROLLBACK');
        throw err;
    }
    finally {
        client.release();
    }
}
//# sourceMappingURL=connection.js.map