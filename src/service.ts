import { createHash, randomUUID } from 'node:crypto';
import { DataSource } from 'typeorm';
import type { EntityManager } from 'typeorm';
import { DomainError, Money, Wallet, WalletLedgerEntry, WagerTransaction } from './domain';
import type { Direction, MoneyProps, Status, WagerKind } from './domain';
import { WalletBalanceChanged, WagerTransactionPendingReference, WagerTransactionProcessed, WagerTransactionRejected } from './events';
import { count, observe } from './metrics';

export interface WagerInput {
  providerId: string; externalTransactionId: string; playerId: string; walletId: string;
  roundId: string; gameId: string; kind: WagerKind; money: MoneyProps;
  referenceExternalTransactionId?: string;
}
export interface ProcessResult { transactionId: string; status: Status; balance: MoneyProps; failureCode?: string; idempotentReplay: boolean }
export interface MessageContext { consumerName: string; messageId: string; payloadHash: string }
type Row = Record<string, any>;

const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
function required(value: unknown, max = 200): value is string { return typeof value === 'string' && value.length > 0 && value.length <= max && value.trim() === value; }
function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value && typeof value === 'object') return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonical((value as Record<string, unknown>)[key])}`).join(',')}}`;
  return JSON.stringify(value);
}
export function sha256(value: unknown): string { return createHash('sha256').update(canonical(value)).digest('hex'); }
function validate(input: WagerInput): { input: WagerInput; hash: string; money: Money } {
  if (!input || !required(input.providerId, 100) || !required(input.externalTransactionId) || !required(input.playerId, 36) || !uuid.test(input.playerId) ||
    !required(input.walletId, 36) || !uuid.test(input.walletId) || !required(input.roundId) || !required(input.gameId) ||
    !['BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK'].includes(input.kind)) throw new DomainError('INVALID_PAYLOAD', 'Invalid wagering fields');
  if ((input.kind === 'REFUND' || input.kind === 'ROLLBACK') && !required(input.referenceExternalTransactionId)) throw new DomainError('INVALID_PAYLOAD', 'Reference is required');
  if (input.referenceExternalTransactionId !== undefined && !required(input.referenceExternalTransactionId)) throw new DomainError('INVALID_PAYLOAD', 'Invalid reference');
  const money = Money.from(input.money);
  if (!money.isPositive()) throw new DomainError('INVALID_MONEY', 'Amount must be positive');
  const normalized: WagerInput = { providerId: input.providerId, externalTransactionId: input.externalTransactionId,
    playerId: input.playerId, walletId: input.walletId, roundId: input.roundId, gameId: input.gameId,
    kind: input.kind, money: money.toJSON() };
  if (input.referenceExternalTransactionId !== undefined) normalized.referenceExternalTransactionId = input.referenceExternalTransactionId;
  return { input: normalized, money, hash: sha256(normalized) };
}
function rowResult(row: Row, replay: boolean): ProcessResult {
  return { transactionId: row.id, status: row.status, balance: Money.from({ amount: String(row.result_balance), currency: row.currency }).toJSON(),
    ...(row.failure_code ? { failureCode: row.failure_code } : {}), idempotentReplay: replay };
}
function event(type: string, walletId: string, transactionId: string, data: Record<string, unknown>): Record<string, unknown> {
  switch (type) {
    case 'WalletBalanceChanged': return WalletBalanceChanged.from(walletId, transactionId, data).toJSON();
    case 'WagerTransactionProcessed': return WagerTransactionProcessed.from(walletId, transactionId, data).toJSON();
    case 'WagerTransactionRejected': return WagerTransactionRejected.from(walletId, transactionId, data).toJSON();
    case 'WagerTransactionPendingReference': return WagerTransactionPendingReference.from(walletId, transactionId, data).toJSON();
    default: throw new DomainError('UNKNOWN_EVENT', 'Unknown integration event');
  }
}

export class WagerService {
  constructor(private readonly ds: DataSource) {}

  async createWallet(playerId: string, initialBalance: MoneyProps): Promise<Row> {
    if (!required(playerId, 36) || !uuid.test(playerId)) throw new DomainError('INVALID_PAYLOAD', 'Invalid playerId');
    const money = Money.from(initialBalance);
    const wallet = Wallet.open(randomUUID(), playerId, money);
    try {
      return await this.ds.transaction(async manager => {
        await manager.query('INSERT INTO wallets(id,player_id,currency,balance,version) VALUES ($1,$2,$3,$4,1)',
          [wallet.id, playerId, wallet.currency, wallet.balance.toString()]);
        if (money.isPositive()) {
          const transactionId = randomUUID();
          await manager.query(`INSERT INTO wager_transactions(id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount,currency,status,result_balance,processed_at)
            VALUES ($1,'__internal__',$2,$3,$4,$5,$6,'__opening__','__opening__','OPENING',$7,$8,'PROCESSED',$7,now())`,
            [transactionId, transactionId, `opening:${wallet.id}`, sha256({ walletId: wallet.id, amount: money.toString() }), wallet.id, playerId, money.toString(), money.currency]);
          await this.insertLedger(manager, wallet.id, transactionId, 'CREDIT', money, Money.zero(money.currency), money);
          await this.enqueue(manager, event('WagerTransactionProcessed', wallet.id, transactionId, { transactionId, status: 'PROCESSED', kind: 'OPENING' }));
          await this.enqueue(manager, event('WalletBalanceChanged', wallet.id, transactionId, { walletId: wallet.id, transactionId,
            direction: 'CREDIT', money: money.toJSON(), balanceBefore: Money.zero(money.currency).toJSON(), balanceAfter: money.toJSON(), walletVersion: 1 }));
        }
        return { id: wallet.id, playerId, balance: money.toJSON(), version: 1 };
      });
    } catch (error) { if (isUnique(error)) throw new DomainError('WALLET_EXISTS', 'Player already has a wallet in this currency'); throw error; }
  }

  async process(raw: WagerInput, idempotencyKey: string, message?: MessageContext): Promise<ProcessResult> {
    if (!required(idempotencyKey, 300)) throw new DomainError('INVALID_PAYLOAD', 'Idempotency-Key is required');
    const { input, hash, money } = validate(raw);
    try {
      return await this.ds.transaction(async manager => {
        const lockStarted = performance.now();
        const wallets: Row[] = await manager.query('SELECT * FROM wallets WHERE id=$1 FOR UPDATE', [input.walletId]);
        const lockWait = performance.now() - lockStarted;
        observe('wager_wallet_lock_wait_ms', lockWait);
        if (lockWait > 50) count('wager_lock_conflicts_total');
        const walletRow = wallets[0];
        if (!walletRow) throw new DomainError('WALLET_NOT_FOUND', 'Wallet does not exist');
        if (message) {
          const inbox: Row[] = await manager.query('SELECT * FROM inbox_messages WHERE consumer_name=$1 AND message_id=$2', [message.consumerName, message.messageId]);
          if (inbox.length && inbox[0]!.payload_hash !== message.payloadHash) throw new DomainError('INBOX_CONFLICT', 'Message ID reused with different payload');
        }
        const existing: Row[] = await manager.query('SELECT * FROM wager_transactions WHERE idempotency_key=$1 OR (provider_id=$2 AND external_transaction_id=$3) LIMIT 2',
          [idempotencyKey, input.providerId, input.externalTransactionId]);
        if (existing.length) {
          if (existing.length !== 1 || existing[0]!.idempotency_key !== idempotencyKey || existing[0]!.payload_hash !== hash) throw new DomainError('IDEMPOTENCY_CONFLICT', 'Key or external transaction ID reused with different payload');
          if (message) await this.insertInbox(manager, message);
          return rowResult(existing[0]!, true);
        }
        if (walletRow.player_id !== input.playerId) throw new DomainError('PLAYER_MISMATCH', 'Wallet belongs to another player');
        if (walletRow.currency !== money.currency) throw new DomainError('CURRENCY_MISMATCH', 'Wallet currency differs');
        if (message) await this.insertInbox(manager, message);
        const transactionId = randomUUID();
        await manager.query(`INSERT INTO wager_transactions(id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount,currency,reference_external_transaction_id,status,result_balance)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'PENDING',$14)`,
          [transactionId, input.providerId, input.externalTransactionId, idempotencyKey, hash, input.walletId, input.playerId,
            input.roundId, input.gameId, input.kind, money.toString(), money.currency, input.referenceExternalTransactionId ?? null, walletRow.balance]);
        const tx: Row = { id: transactionId, ...snakeInput(input), amount: money.toString(), currency: money.currency,
          status: 'PENDING', result_balance: walletRow.balance, attempts: 0 };
        const result = await this.applyTransaction(manager, walletRow, tx, money);
        return result;
      });
    } catch (error) {
      // A different wallet can race for the same global key. After the failed transaction, read its winner.
      if (isUnique(error)) {
        const rows: Row[] = await this.ds.query('SELECT * FROM wager_transactions WHERE idempotency_key=$1 OR (provider_id=$2 AND external_transaction_id=$3)',
          [idempotencyKey, input.providerId, input.externalTransactionId]);
        if (rows.length === 1 && rows[0]!.idempotency_key === idempotencyKey && rows[0]!.payload_hash === hash)
          return message ? this.process(input, idempotencyKey, message) : rowResult(rows[0]!, true);
        throw new DomainError('IDEMPOTENCY_CONFLICT', 'Key or external transaction ID reused with different payload');
      }
      throw error;
    }
  }

  private async applyTransaction(manager: EntityManager, walletRow: Row, tx: Row, money: Money): Promise<ProcessResult> {
    const wallet = Wallet.rehydrate(walletRow.id, walletRow.player_id, Money.from({ amount: String(walletRow.balance), currency: walletRow.currency }), walletRow.version);
    const aggregate = WagerTransaction.rehydrate({ id: tx.id, kind: tx.kind, money, payloadHash: tx.payload_hash ?? '',
      referenceExternalTransactionId: tx.reference_external_transaction_id, status: tx.status });
    let reference: Row | undefined;
    if (tx.reference_external_transaction_id) {
      const refs: Row[] = await manager.query('SELECT * FROM wager_transactions WHERE provider_id=$1 AND external_transaction_id=$2',
        [tx.provider_id, tx.reference_external_transaction_id]);
      reference = refs[0];
      if (!reference) {
        const attempts = tx.attempts + 1;
        if (attempts >= 8) return this.reject(manager, tx, wallet.balance, 'REFERENCE_NOT_FOUND');
        aggregate.markPendingReference();
        const delay = Math.min(2 ** (attempts - 1), 60);
        await manager.query(`UPDATE wager_transactions SET status='PENDING_REFERENCE',attempts=$2,next_attempt_at=now()+($3::text || ' seconds')::interval,result_balance=$4 WHERE id=$1`,
          [tx.id, attempts, delay, wallet.balance.toString()]);
        await this.enqueue(manager, event('WagerTransactionPendingReference', wallet.id, tx.id, { transactionId: tx.id, referenceExternalTransactionId: tx.reference_external_transaction_id }));
        return { transactionId: tx.id, status: 'PENDING_REFERENCE', balance: wallet.balance.toJSON(), idempotentReplay: false };
      }
      if (reference.status === 'PENDING' || reference.status === 'PENDING_REFERENCE') {
        const attempts = tx.attempts + 1;
        if (attempts >= 8) return this.reject(manager, tx, wallet.balance, 'REFERENCE_NOT_PROCESSED');
        aggregate.markPendingReference();
        await manager.query(`UPDATE wager_transactions SET status='PENDING_REFERENCE',attempts=$2,next_attempt_at=now()+($3::text || ' seconds')::interval,result_balance=$4 WHERE id=$1`,
          [tx.id, attempts, Math.min(2 ** (attempts - 1), 60), wallet.balance.toString()]);
        await this.enqueue(manager, event('WagerTransactionPendingReference', wallet.id, tx.id, { transactionId: tx.id, referenceExternalTransactionId: tx.reference_external_transaction_id }));
        return { transactionId: tx.id, status: 'PENDING_REFERENCE', balance: wallet.balance.toJSON(), idempotentReplay: false };
      }
      if (reference.status !== 'PROCESSED') return this.reject(manager, tx, wallet.balance, 'REFERENCE_NOT_PROCESSED');
      if (reference.player_id !== tx.player_id || reference.wallet_id !== tx.wallet_id || reference.currency !== tx.currency || reference.round_id !== tx.round_id) return this.reject(manager, tx, wallet.balance, 'REFERENCE_MISMATCH');
      if (tx.kind === 'REFUND' && reference.kind !== 'BET' || tx.kind === 'ROLLBACK' && !['BET','WIN','REFUND'].includes(reference.kind) || tx.kind === 'WIN' && reference.kind !== 'BET')
        return this.reject(manager, tx, wallet.balance, 'INVALID_REFERENCE_KIND');
      if (tx.kind !== 'WIN' && !money.equals(Money.from({ amount: String(reference.amount), currency: reference.currency }))) return this.reject(manager, tx, wallet.balance, 'REFERENCE_AMOUNT_MISMATCH');
      if (tx.kind === 'REFUND' || tx.kind === 'ROLLBACK') {
        const reversed: Row[] = await manager.query(`SELECT id FROM wager_transactions WHERE reference_transaction_id=$1 AND kind=$2 AND status='PROCESSED'`, [reference.id, tx.kind]);
        if (reversed.length) return this.reject(manager, tx, wallet.balance, 'ALREADY_REVERSED');
      }
    }
    const direction = aggregate.ledgerDirectionFor(reference ? WagerTransaction.rehydrate({ id: reference.id, kind: reference.kind,
      money: Money.from({ amount: String(reference.amount), currency: reference.currency }), payloadHash: reference.payload_hash,
      referenceExternalTransactionId: reference.reference_external_transaction_id, status: reference.status }) : undefined);
    if (!direction) {
      aggregate.markProcessed(reference?.id, new Date());
      await manager.query(`UPDATE wager_transactions SET status='PROCESSED',result_balance=$2,processed_at=now(),next_attempt_at=NULL WHERE id=$1`, [tx.id, wallet.balance.toString()]);
      await this.enqueue(manager, event('WagerTransactionProcessed', wallet.id, tx.id, { transactionId: tx.id, status: 'PROCESSED', kind: tx.kind }));
      return { transactionId: tx.id, status: 'PROCESSED', balance: wallet.balance.toJSON(), idempotentReplay: false };
    }
    let balances: { before: Money; after: Money };
    try { balances = wallet.apply(direction, money, tx.kind === 'ROLLBACK' ? 'REVERSAL_INSUFFICIENT_FUNDS' : 'INSUFFICIENT_FUNDS'); }
    catch (error) { if (error instanceof DomainError && ['INSUFFICIENT_FUNDS','REVERSAL_INSUFFICIENT_FUNDS'].includes(error.code)) return this.reject(manager, tx, wallet.balance, error.code); throw error; }
    const entry = WalletLedgerEntry.create(randomUUID(), wallet.id, tx.id, direction, money, balances.before, balances.after);
    aggregate.markProcessed(reference?.id, new Date());
    await manager.query('UPDATE wallets SET balance=$2,version=$3,updated_at=now() WHERE id=$1', [wallet.id, wallet.balance.toString(), wallet.version]);
    await this.insertLedger(manager, entry.walletId, entry.transactionId, entry.direction, entry.money, entry.balanceBefore, entry.balanceAfter, entry.id);
    await manager.query(`UPDATE wager_transactions SET status='PROCESSED',reference_transaction_id=$2,result_balance=$3,processed_at=now(),next_attempt_at=NULL WHERE id=$1`,
      [tx.id, reference?.id ?? null, wallet.balance.toString()]);
    await this.enqueue(manager, event('WagerTransactionProcessed', wallet.id, tx.id, { transactionId: tx.id, status: 'PROCESSED', kind: tx.kind }));
    await this.enqueue(manager, event('WalletBalanceChanged', wallet.id, tx.id, { walletId: wallet.id, transactionId: tx.id, direction,
      money: money.toJSON(), balanceBefore: balances.before.toJSON(), balanceAfter: balances.after.toJSON(), walletVersion: wallet.version }));
    return { transactionId: tx.id, status: 'PROCESSED', balance: wallet.balance.toJSON(), idempotentReplay: false };
  }

  private async reject(manager: EntityManager, tx: Row, balance: Money, code: string): Promise<ProcessResult> {
    WagerTransaction.rehydrate({ id: tx.id, kind: tx.kind, money: Money.from({ amount: String(tx.amount), currency: tx.currency }),
      payloadHash: tx.payload_hash ?? '', referenceExternalTransactionId: tx.reference_external_transaction_id, status: tx.status }).reject(code);
    await manager.query(`UPDATE wager_transactions SET status='REJECTED',failure_code=$2,result_balance=$3,processed_at=now(),next_attempt_at=NULL WHERE id=$1`, [tx.id, code, balance.toString()]);
    await this.enqueue(manager, event('WagerTransactionRejected', tx.wallet_id, tx.id, { transactionId: tx.id, failureCode: code }));
    return { transactionId: tx.id, status: 'REJECTED', balance: balance.toJSON(), failureCode: code, idempotentReplay: false };
  }
  private async insertLedger(manager: EntityManager, walletId: string, transactionId: string, direction: Direction, money: Money, before: Money, after: Money, id: string = randomUUID()): Promise<void> {
    await manager.query(`INSERT INTO wallet_ledger(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, [id, walletId, transactionId, direction, money.toString(), money.currency, before.toString(), after.toString()]);
  }
  private async enqueue(manager: EntityManager, payload: Record<string, unknown>): Promise<void> {
    await manager.query('INSERT INTO outbox_messages(id,aggregate_id,event_type,payload) VALUES ($1,$2,$3,$4)',
      [payload.eventId, payload.aggregateId, payload.eventType, JSON.stringify(payload)]);
  }
  private async insertInbox(manager: EntityManager, message: MessageContext): Promise<void> {
    const inserted: Row[] = await manager.query(`INSERT INTO inbox_messages(consumer_name,message_id,payload_hash) VALUES ($1,$2,$3)
      ON CONFLICT (consumer_name,message_id) DO NOTHING RETURNING payload_hash`,
      [message.consumerName, message.messageId, message.payloadHash]);
    if (!inserted.length) {
      const rows: Row[] = await manager.query('SELECT payload_hash FROM inbox_messages WHERE consumer_name=$1 AND message_id=$2',
        [message.consumerName, message.messageId]);
      if (rows[0]?.payload_hash !== message.payloadHash) throw new DomainError('INBOX_CONFLICT', 'Message ID reused with different payload');
    }
  }

  async retryPending(limit = 50): Promise<void> {
    const rows: Row[] = await this.ds.query(`SELECT id,wallet_id FROM wager_transactions WHERE status='PENDING_REFERENCE' AND next_attempt_at<=now() ORDER BY next_attempt_at,id LIMIT $1`, [limit]);
    for (const candidate of rows) {
      await this.ds.transaction(async manager => {
        const wallets: Row[] = await manager.query('SELECT * FROM wallets WHERE id=$1 FOR UPDATE', [candidate.wallet_id]);
        const txs: Row[] = await manager.query('SELECT * FROM wager_transactions WHERE id=$1 FOR UPDATE', [candidate.id]);
        const tx = txs[0];
        if (!tx || tx.status !== 'PENDING_REFERENCE' || new Date(tx.next_attempt_at) > new Date()) return;
        await this.applyTransaction(manager, wallets[0]!, tx, Money.from({ amount: String(tx.amount), currency: tx.currency }));
      });
    }
  }

  async getWallet(id: string): Promise<Row> {
    if (!uuid.test(id)) throw new DomainError('INVALID_PAYLOAD', 'Invalid wallet ID');
    const rows: Row[] = await this.ds.query('SELECT * FROM wallets WHERE id=$1', [id]);
    if (!rows[0]) throw new DomainError('WALLET_NOT_FOUND', 'Wallet does not exist');
    return { id: rows[0].id, playerId: rows[0].player_id, balance: Money.from({ amount: String(rows[0].balance), currency: rows[0].currency }).toJSON(), version: rows[0].version };
  }
  async getTransaction(id: string): Promise<Row> {
    if (!uuid.test(id)) throw new DomainError('INVALID_PAYLOAD', 'Invalid transaction ID');
    const rows: Row[] = await this.ds.query('SELECT * FROM wager_transactions WHERE id=$1', [id]);
    if (!rows[0]) throw new DomainError('TRANSACTION_NOT_FOUND', 'Transaction does not exist');
    return transactionView(rows[0]);
  }
  async getProviderTransaction(providerId: string, externalId: string): Promise<Row> {
    const rows: Row[] = await this.ds.query('SELECT * FROM wager_transactions WHERE provider_id=$1 AND external_transaction_id=$2', [providerId, externalId]);
    if (!rows[0]) throw new DomainError('TRANSACTION_NOT_FOUND', 'Transaction does not exist');
    return transactionView(rows[0]);
  }
  async ledger(walletId: string, cursor?: string, limit = 50): Promise<Row> {
    if (!uuid.test(walletId) || !Number.isInteger(limit) || limit < 1 || limit > 100) throw new DomainError('INVALID_PAYLOAD', 'Invalid pagination');
    let after: { createdAt: string; id: string } | undefined;
    if (cursor) {
      try { after = JSON.parse(Buffer.from(cursor, 'base64url').toString()); if (!after || !uuid.test(after.id) || Number.isNaN(Date.parse(after.createdAt))) throw Error(); }
      catch { throw new DomainError('INVALID_PAYLOAD', 'Invalid cursor'); }
    }
    const rows: Row[] = await this.ds.query(`SELECT id,transaction_id,direction,amount,currency,balance_before,balance_after,created_at FROM wallet_ledger
      WHERE wallet_id=$1 AND ($2::timestamptz IS NULL OR (created_at,id)>($2::timestamptz,$3::uuid)) ORDER BY created_at,id LIMIT $4`,
      [walletId, after?.createdAt ?? null, after?.id ?? null, limit + 1]);
    const page = rows.slice(0, limit);
    return { entries: page.map(row => ({ id: row.id, transactionId: row.transaction_id, direction: row.direction,
      money: { amount: row.amount, currency: row.currency }, balanceBefore: { amount: row.balance_before, currency: row.currency },
      balanceAfter: { amount: row.balance_after, currency: row.currency }, createdAt: row.created_at })),
      nextCursor: rows.length > limit ? Buffer.from(JSON.stringify({ createdAt: page.at(-1)!.created_at, id: page.at(-1)!.id })).toString('base64url') : null };
  }
  async reconcile(walletId: string): Promise<Row> {
    if (!uuid.test(walletId)) throw new DomainError('INVALID_PAYLOAD', 'Invalid wallet ID');
    const result = await this.ds.transaction('REPEATABLE READ', async manager => {
      const wallets: Row[] = await manager.query('SELECT * FROM wallets WHERE id=$1', [walletId]);
      const wallet = wallets[0];
      if (!wallet) throw new DomainError('WALLET_NOT_FOUND', 'Wallet does not exist');
      const rows: Row[] = await manager.query(`SELECT COALESCE(SUM(CASE WHEN direction='CREDIT' THEN amount ELSE -amount END),0)::text AS total,
        COUNT(*)::integer AS count FROM wallet_ledger WHERE wallet_id=$1`, [walletId]);
      const calculated = Money.signed(rows[0]!.total, wallet.currency);
      const stored = Money.from({ amount: String(wallet.balance), currency: wallet.currency });
      const difference = stored.subtract(calculated);
      return { walletId, storedBalance: stored.toJSON(), calculatedBalance: calculated.toJSON(), difference: difference.toJSON(),
        consistent: difference.isZero(), checkedEntries: rows[0]!.count };
    });
    if (!result.consistent) {
      count('wager_reconciliation_mismatches_total');
      console.error(JSON.stringify({ level: 'error', event: 'reconciliation_mismatch', walletId }));
    }
    return result;
  }
}

function snakeInput(input: WagerInput): Row { return { provider_id: input.providerId, wallet_id: input.walletId, player_id: input.playerId,
  round_id: input.roundId, kind: input.kind, reference_external_transaction_id: input.referenceExternalTransactionId }; }
function transactionView(row: Row): Row { return { transactionId: row.id, providerId: row.provider_id, externalTransactionId: row.external_transaction_id,
  walletId: row.wallet_id, playerId: row.player_id, roundId: row.round_id, gameId: row.game_id, kind: row.kind,
  money: { amount: row.amount, currency: row.currency }, status: row.status, failureCode: row.failure_code,
  balance: { amount: row.result_balance, currency: row.currency }, referenceExternalTransactionId: row.reference_external_transaction_id,
  createdAt: row.created_at, processedAt: row.processed_at }; }
function isUnique(error: unknown): boolean { return Boolean(error && typeof error === 'object' && 'driverError' in error && (error as any).driverError?.code === '23505'); }
