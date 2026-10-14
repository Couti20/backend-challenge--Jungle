import { DataSource, EntitySchema } from 'typeorm';

// EntitySchema keeps persistence metadata outside the domain classes.
export const WalletRecord = new EntitySchema({ name: 'WalletRecord', tableName: 'wallets', columns: {
  id: { type: 'uuid', primary: true }, playerId: { name: 'player_id', type: 'uuid' }, currency: { type: 'varchar', length: 3 },
  balance: { type: 'numeric', precision: 20, scale: 2 }, version: { type: 'integer' },
  createdAt: { name: 'created_at', type: 'timestamptz' }, updatedAt: { name: 'updated_at', type: 'timestamptz' },
} });

export const TransactionRecord = new EntitySchema({ name: 'TransactionRecord', tableName: 'wager_transactions', columns: {
  id: { type: 'uuid', primary: true }, providerId: { name: 'provider_id', type: 'varchar' }, externalTransactionId: { name: 'external_transaction_id', type: 'varchar' },
  idempotencyKey: { name: 'idempotency_key', type: 'varchar' }, payloadHash: { name: 'payload_hash', type: 'varchar' },
  walletId: { name: 'wallet_id', type: 'uuid' }, playerId: { name: 'player_id', type: 'uuid' }, roundId: { name: 'round_id', type: 'varchar' }, gameId: { name: 'game_id', type: 'varchar' },
  kind: { type: 'varchar' }, amount: { type: 'numeric', precision: 20, scale: 2 }, currency: { type: 'varchar', length: 3 },
  referenceExternalTransactionId: { name: 'reference_external_transaction_id', type: 'varchar', nullable: true },
  referenceTransactionId: { name: 'reference_transaction_id', type: 'uuid', nullable: true }, status: { type: 'varchar' },
  failureCode: { name: 'failure_code', type: 'varchar', nullable: true }, resultBalance: { name: 'result_balance', type: 'numeric', precision: 20, scale: 2, nullable: true },
  attempts: { type: 'integer' }, nextAttemptAt: { name: 'next_attempt_at', type: 'timestamptz', nullable: true },
  createdAt: { name: 'created_at', type: 'timestamptz' }, processedAt: { name: 'processed_at', type: 'timestamptz', nullable: true },
} });

export function makeDataSource(): DataSource {
  return new DataSource({ type: 'postgres', url: process.env.DATABASE_URL ?? 'postgres://wager:wager@localhost:5432/wager',
    entities: [WalletRecord, TransactionRecord], synchronize: false, logging: false, poolSize: 20,
    extra: { connectionTimeoutMillis: 3000 } });
}
