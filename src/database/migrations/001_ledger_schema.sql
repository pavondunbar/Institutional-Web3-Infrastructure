-- Migration: 001_ledger_schema.sql
-- Enterprise-grade append-only, double-entry ledger with ACID guarantees
-- Hash-chained audit trail, transactional outbox, wallet and indexer tables

BEGIN;

-- ============================================================
-- ROLES: Least-privilege permissions
-- ============================================================
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ledger_reader') THEN
    CREATE ROLE ledger_reader;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ledger_writer') THEN
    CREATE ROLE ledger_writer;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_reader') THEN
    CREATE USER app_reader WITH PASSWORD 'reader_password';
    GRANT ledger_reader TO app_reader;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_writer') THEN
    CREATE USER app_writer WITH PASSWORD 'writer_password';
    GRANT ledger_writer TO app_writer;
  END IF;
END $$;

-- ============================================================
-- EXTENSIONS
-- ============================================================
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================
-- ACCOUNTS TABLE
-- ============================================================
CREATE TABLE accounts (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  external_id VARCHAR(255) UNIQUE NOT NULL,
  account_type VARCHAR(50) NOT NULL CHECK (account_type IN ('asset', 'liability', 'equity', 'revenue', 'expense')),
  currency VARCHAR(10) NOT NULL,
  status VARCHAR(20) NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'frozen', 'closed')),
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_accounts_external_id ON accounts(external_id);
CREATE INDEX idx_accounts_type ON accounts(account_type);

-- ============================================================
-- JOURNAL ENTRIES (grouped transactions - double entry headers)
-- ============================================================
CREATE TABLE journal_entries (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  idempotency_key VARCHAR(255) UNIQUE NOT NULL,
  description TEXT,
  external_ref VARCHAR(255),
  external_ref_type VARCHAR(50),
  status VARCHAR(20) NOT NULL DEFAULT 'posted' CHECK (status IN ('posted', 'reversed')),
  reversed_by UUID REFERENCES journal_entries(id),
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_journal_idempotency ON journal_entries(idempotency_key);
CREATE INDEX idx_journal_external_ref ON journal_entries(external_ref);
CREATE INDEX idx_journal_created_at ON journal_entries(created_at);

-- ============================================================
-- LEDGER ENTRIES (individual debit/credit lines - append only)
-- ============================================================
CREATE TABLE ledger_entries (
  id BIGSERIAL PRIMARY KEY,
  journal_entry_id UUID NOT NULL REFERENCES journal_entries(id),
  account_id UUID NOT NULL REFERENCES accounts(id),
  amount BIGINT NOT NULL CHECK (amount != 0),
  direction VARCHAR(6) NOT NULL CHECK (direction IN ('debit', 'credit')),
  -- Hash chain: SHA-256 of (prev_hash || entry data)
  entry_hash CHAR(64) NOT NULL,
  prev_hash CHAR(64) NOT NULL,
  sequence_num BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_ledger_journal ON ledger_entries(journal_entry_id);
CREATE INDEX idx_ledger_account ON ledger_entries(account_id);
CREATE INDEX idx_ledger_account_seq ON ledger_entries(account_id, sequence_num);
CREATE INDEX idx_ledger_created_at ON ledger_entries(created_at);

-- Unique constraint: one sequence number per account (hash chain integrity)
ALTER TABLE ledger_entries ADD CONSTRAINT uq_ledger_account_seq UNIQUE (account_id, sequence_num);

-- ============================================================
-- BALANCE CACHE (derived, can be reconstructed from ledger)
-- ============================================================
CREATE TABLE balance_cache (
  account_id UUID PRIMARY KEY REFERENCES accounts(id),
  debit_total BIGINT NOT NULL DEFAULT 0 CHECK (debit_total >= 0),
  credit_total BIGINT NOT NULL DEFAULT 0 CHECK (credit_total >= 0),
  balance BIGINT NOT NULL DEFAULT 0,
  last_entry_id BIGINT REFERENCES ledger_entries(id),
  last_entry_hash CHAR(64),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- TRANSACTIONAL OUTBOX (for Kafka publishing)
-- ============================================================
CREATE TABLE outbox (
  id BIGSERIAL PRIMARY KEY,
  aggregate_type VARCHAR(100) NOT NULL,
  aggregate_id VARCHAR(255) NOT NULL,
  event_type VARCHAR(100) NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at TIMESTAMPTZ,
  published BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_outbox_unpublished ON outbox(published, created_at) WHERE published = FALSE;

-- ============================================================
-- WALLET TABLES
-- ============================================================
CREATE TABLE wallets (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  account_id UUID REFERENCES accounts(id),
  chain VARCHAR(50) NOT NULL,
  address VARCHAR(255) NOT NULL,
  wallet_type VARCHAR(30) NOT NULL CHECK (wallet_type IN ('hot', 'warm', 'cold', 'deposit')),
  key_id VARCHAR(255), -- reference to KMS/HSM key
  status VARCHAR(20) NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled', 'archived')),
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_wallets_chain_address ON wallets(chain, address);
CREATE INDEX idx_wallets_account ON wallets(account_id);

CREATE TABLE transactions_blockchain (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  wallet_id UUID NOT NULL REFERENCES wallets(id),
  journal_entry_id UUID REFERENCES journal_entries(id),
  chain VARCHAR(50) NOT NULL,
  tx_hash VARCHAR(255),
  from_address VARCHAR(255) NOT NULL,
  to_address VARCHAR(255) NOT NULL,
  amount BIGINT NOT NULL,
  gas_limit BIGINT,
  gas_price BIGINT,
  gas_used BIGINT,
  nonce BIGINT,
  status VARCHAR(30) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'signing', 'submitted', 'confirmed', 'failed', 'dropped', 'reorged')),
  confirmations INT NOT NULL DEFAULT 0,
  block_number BIGINT,
  block_hash VARCHAR(255),
  submitted_at TIMESTAMPTZ,
  confirmed_at TIMESTAMPTZ,
  error_message TEXT,
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_blockchain_tx_hash ON transactions_blockchain(tx_hash);
CREATE INDEX idx_blockchain_tx_wallet ON transactions_blockchain(wallet_id);
CREATE INDEX idx_blockchain_tx_status ON transactions_blockchain(status);
CREATE INDEX idx_blockchain_tx_block ON transactions_blockchain(block_number);

-- ============================================================
-- INDEXER TABLES
-- ============================================================
CREATE TABLE indexed_blocks (
  id BIGSERIAL PRIMARY KEY,
  chain VARCHAR(50) NOT NULL,
  block_number BIGINT NOT NULL,
  block_hash VARCHAR(255) NOT NULL,
  parent_hash VARCHAR(255) NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  tx_count INT NOT NULL DEFAULT 0,
  status VARCHAR(20) NOT NULL DEFAULT 'confirmed' CHECK (status IN ('confirmed', 'reorged')),
  indexed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_blocks_chain_number ON indexed_blocks(chain, block_number) WHERE status = 'confirmed';
CREATE INDEX idx_blocks_chain_hash ON indexed_blocks(chain, block_hash);

CREATE TABLE indexed_events (
  id BIGSERIAL PRIMARY KEY,
  block_id BIGINT NOT NULL REFERENCES indexed_blocks(id),
  chain VARCHAR(50) NOT NULL,
  tx_hash VARCHAR(255) NOT NULL,
  log_index INT NOT NULL,
  contract_address VARCHAR(255) NOT NULL,
  event_signature VARCHAR(255) NOT NULL,
  topics JSONB NOT NULL DEFAULT '[]',
  data BYTEA,
  decoded JSONB,
  processed BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_events_contract ON indexed_events(contract_address, event_signature);
CREATE INDEX idx_events_tx ON indexed_events(tx_hash);
CREATE INDEX idx_events_unprocessed ON indexed_events(processed, created_at) WHERE processed = FALSE;

-- ============================================================
-- RECONCILIATION
-- ============================================================
CREATE TABLE reconciliation_runs (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  run_type VARCHAR(50) NOT NULL CHECK (run_type IN ('balance', 'hash_chain', 'cross_system')),
  status VARCHAR(20) NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'passed', 'failed')),
  accounts_checked INT NOT NULL DEFAULT 0,
  discrepancies_found INT NOT NULL DEFAULT 0,
  details JSONB DEFAULT '{}',
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ
);

-- ============================================================
-- TRIGGERS: Append-only enforcement (NO UPDATE/DELETE on ledger)
-- ============================================================
CREATE OR REPLACE FUNCTION prevent_ledger_mutation() RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'Ledger entries are append-only. UPDATE and DELETE are prohibited.';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_ledger_no_update
  BEFORE UPDATE ON ledger_entries
  FOR EACH ROW EXECUTE FUNCTION prevent_ledger_mutation();

CREATE TRIGGER trg_ledger_no_delete
  BEFORE DELETE ON ledger_entries
  FOR EACH ROW EXECUTE FUNCTION prevent_ledger_mutation();

CREATE TRIGGER trg_journal_no_delete
  BEFORE DELETE ON journal_entries
  FOR EACH ROW EXECUTE FUNCTION prevent_ledger_mutation();

-- Journal entries can only be updated to set status='reversed'
CREATE OR REPLACE FUNCTION restrict_journal_update() RETURNS TRIGGER AS $$
BEGIN
  IF OLD.status = 'reversed' THEN
    RAISE EXCEPTION 'Cannot modify a reversed journal entry.';
  END IF;
  IF NEW.status != 'reversed' OR NEW.id != OLD.id OR NEW.idempotency_key != OLD.idempotency_key THEN
    RAISE EXCEPTION 'Journal entries can only be updated to mark as reversed.';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_journal_restrict_update
  BEFORE UPDATE ON journal_entries
  FOR EACH ROW EXECUTE FUNCTION restrict_journal_update();

-- ============================================================
-- TRIGGER: Balanced journal entries (zero-sum constraint)
-- Deferred to end of transaction for atomic posting
-- ============================================================
CREATE OR REPLACE FUNCTION check_journal_balanced() RETURNS TRIGGER AS $$
DECLARE
  debit_sum BIGINT;
  credit_sum BIGINT;
BEGIN
  SELECT
    COALESCE(SUM(CASE WHEN direction = 'debit' THEN amount ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN direction = 'credit' THEN amount ELSE 0 END), 0)
  INTO debit_sum, credit_sum
  FROM ledger_entries
  WHERE journal_entry_id = NEW.journal_entry_id;

  IF debit_sum != credit_sum THEN
    RAISE EXCEPTION 'Journal entry % is unbalanced: debits=% credits=%',
      NEW.journal_entry_id, debit_sum, credit_sum;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER trg_check_balanced
  AFTER INSERT ON ledger_entries
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION check_journal_balanced();

-- ============================================================
-- TRIGGER: Hash chain integrity validation
-- ============================================================
CREATE OR REPLACE FUNCTION validate_hash_chain() RETURNS TRIGGER AS $$
DECLARE
  expected_prev_hash CHAR(64);
  expected_seq BIGINT;
BEGIN
  -- Get the latest entry for this account
  SELECT entry_hash, sequence_num
  INTO expected_prev_hash, expected_seq
  FROM ledger_entries
  WHERE account_id = NEW.account_id
  ORDER BY sequence_num DESC
  LIMIT 1;

  IF expected_prev_hash IS NULL THEN
    -- First entry for account
    IF NEW.prev_hash != '0000000000000000000000000000000000000000000000000000000000000000' THEN
      RAISE EXCEPTION 'First entry must have zero prev_hash';
    END IF;
    IF NEW.sequence_num != 1 THEN
      RAISE EXCEPTION 'First entry must have sequence_num = 1';
    END IF;
  ELSE
    IF NEW.prev_hash != expected_prev_hash THEN
      RAISE EXCEPTION 'Hash chain broken: expected prev_hash=% got=%', expected_prev_hash, NEW.prev_hash;
    END IF;
    IF NEW.sequence_num != expected_seq + 1 THEN
      RAISE EXCEPTION 'Sequence gap: expected=% got=%', expected_seq + 1, NEW.sequence_num;
    END IF;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_validate_hash_chain
  BEFORE INSERT ON ledger_entries
  FOR EACH ROW EXECUTE FUNCTION validate_hash_chain();

-- ============================================================
-- PERMISSIONS: Least-privilege
-- ============================================================
-- Reader: SELECT only
GRANT USAGE ON SCHEMA public TO ledger_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ledger_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ledger_reader;

-- Writer: INSERT on ledger tables, SELECT + UPDATE on operational tables
GRANT USAGE ON SCHEMA public TO ledger_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ledger_writer;
GRANT INSERT ON journal_entries, ledger_entries, outbox TO ledger_writer;
GRANT INSERT, UPDATE ON accounts, balance_cache, wallets, transactions_blockchain TO ledger_writer;
GRANT INSERT, UPDATE ON indexed_blocks, indexed_events, reconciliation_runs TO ledger_writer;
GRANT UPDATE ON journal_entries TO ledger_writer; -- only for reversal marking
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO ledger_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ledger_writer;

COMMIT;
