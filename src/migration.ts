export const up = `
CREATE TABLE wallets (
  id uuid PRIMARY KEY, player_id uuid NOT NULL, currency varchar(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  balance numeric(20,2) NOT NULL DEFAULT 0 CHECK (balance >= 0),
  version integer NOT NULL DEFAULT 1 CHECK (version >= 1),
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (player_id, currency)
);
CREATE TABLE wager_transactions (
  id uuid PRIMARY KEY, provider_id varchar(100) NOT NULL, external_transaction_id varchar(200) NOT NULL,
  idempotency_key varchar(300) NOT NULL UNIQUE, payload_hash char(64) NOT NULL,
  wallet_id uuid NOT NULL REFERENCES wallets(id), player_id uuid NOT NULL,
  round_id varchar(200) NOT NULL, game_id varchar(200) NOT NULL,
  kind varchar(12) NOT NULL CHECK (kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
  amount numeric(20,2) NOT NULL CHECK (amount > 0), currency varchar(3) NOT NULL,
  reference_external_transaction_id varchar(200), reference_transaction_id uuid REFERENCES wager_transactions(id),
  status varchar(20) NOT NULL CHECK (status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
  failure_code varchar(100), result_balance numeric(20,2) CHECK (result_balance >= 0),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0), next_attempt_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(), processed_at timestamptz,
  UNIQUE (provider_id, external_transaction_id),
  CHECK (kind NOT IN ('REFUND','ROLLBACK') OR reference_external_transaction_id IS NOT NULL),
  CHECK (status != 'REJECTED' OR failure_code IS NOT NULL)
);
CREATE UNIQUE INDEX wager_one_processed_reversal ON wager_transactions(reference_transaction_id,kind)
  WHERE kind IN ('REFUND','ROLLBACK') AND status = 'PROCESSED';
CREATE INDEX wager_pending_due ON wager_transactions(next_attempt_at,id) WHERE status = 'PENDING_REFERENCE';
CREATE TABLE wallet_ledger (
  id uuid PRIMARY KEY, wallet_id uuid NOT NULL REFERENCES wallets(id),
  transaction_id uuid NOT NULL UNIQUE REFERENCES wager_transactions(id),
  direction varchar(6) NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
  amount numeric(20,2) NOT NULL CHECK (amount > 0), currency varchar(3) NOT NULL,
  balance_before numeric(20,2) NOT NULL CHECK (balance_before >= 0),
  balance_after numeric(20,2) NOT NULL CHECK (balance_after >= 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((direction = 'CREDIT' AND balance_before + amount = balance_after)
      OR (direction = 'DEBIT' AND balance_before - amount = balance_after))
);
CREATE INDEX ledger_wallet_cursor ON wallet_ledger(wallet_id,created_at,id);
CREATE FUNCTION reject_ledger_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'ledger is append only'; END $$;
CREATE TRIGGER ledger_immutable BEFORE UPDATE OR DELETE ON wallet_ledger
  FOR EACH ROW EXECUTE FUNCTION reject_ledger_mutation();
CREATE TABLE inbox_messages (
  consumer_name varchar(100) NOT NULL, message_id varchar(200) NOT NULL,
  payload_hash char(64) NOT NULL, received_at timestamptz NOT NULL DEFAULT now(),
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_name,message_id)
);
CREATE TABLE outbox_messages (
  id uuid PRIMARY KEY, aggregate_id uuid NOT NULL, event_type varchar(100) NOT NULL,
  payload jsonb NOT NULL, occurred_at timestamptz NOT NULL DEFAULT now(),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_attempt_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz
);
CREATE INDEX outbox_due ON outbox_messages(next_attempt_at,id) WHERE published_at IS NULL;
`;

export const down = `
DROP TABLE outbox_messages;
DROP TABLE inbox_messages;
DROP TABLE wallet_ledger;
DROP FUNCTION reject_ledger_mutation();
DROP TABLE wager_transactions;
DROP TABLE wallets;
`;
