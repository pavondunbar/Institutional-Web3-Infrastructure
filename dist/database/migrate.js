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
const fs = __importStar(require("fs"));
const path = __importStar(require("path"));
const pg_1 = require("pg");
const config_1 = require("../config");
async function migrate() {
    // Use a privileged connection for migrations
    const pool = new pg_1.Pool({
        host: config_1.config.postgres.host,
        port: config_1.config.postgres.port,
        database: config_1.config.postgres.database,
        user: process.env.PG_ADMIN_USER || 'postgres',
        password: process.env.PG_ADMIN_PASSWORD || 'postgres',
    });
    const client = await pool.connect();
    try {
        // Create migrations tracking table
        await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        id SERIAL PRIMARY KEY,
        filename VARCHAR(255) UNIQUE NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
        const migrationsDir = path.join(__dirname, 'migrations');
        const files = fs.readdirSync(migrationsDir).filter(f => f.endsWith('.sql')).sort();
        for (const file of files) {
            const { rowCount } = await client.query('SELECT 1 FROM schema_migrations WHERE filename = $1', [file]);
            if (rowCount && rowCount > 0) {
                config_1.logger.info({ file }, 'Migration already applied');
                continue;
            }
            const sql = fs.readFileSync(path.join(migrationsDir, file), 'utf8');
            config_1.logger.info({ file }, 'Applying migration');
            await client.query(sql);
            await client.query('INSERT INTO schema_migrations (filename) VALUES ($1)', [file]);
            config_1.logger.info({ file }, 'Migration applied');
        }
    }
    finally {
        client.release();
        await pool.end();
    }
}
migrate().catch((err) => {
    config_1.logger.fatal(err, 'Migration failed');
    process.exit(1);
});
//# sourceMappingURL=migrate.js.map