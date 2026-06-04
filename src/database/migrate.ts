import * as fs from 'fs';
import * as path from 'path';
import { Pool } from 'pg';
import { config, logger } from '../config';

async function migrate() {
  // Use a privileged connection for migrations
  const pool = new Pool({
    host: config.postgres.host,
    port: config.postgres.port,
    database: config.postgres.database,
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
      const { rowCount } = await client.query(
        'SELECT 1 FROM schema_migrations WHERE filename = $1', [file]
      );
      if (rowCount && rowCount > 0) {
        logger.info({ file }, 'Migration already applied');
        continue;
      }

      const sql = fs.readFileSync(path.join(migrationsDir, file), 'utf8');
      logger.info({ file }, 'Applying migration');
      await client.query(sql);
      await client.query('INSERT INTO schema_migrations (filename) VALUES ($1)', [file]);
      logger.info({ file }, 'Migration applied');
    }
  } finally {
    client.release();
    await pool.end();
  }
}

migrate().catch((err) => {
  logger.fatal(err, 'Migration failed');
  process.exit(1);
});
