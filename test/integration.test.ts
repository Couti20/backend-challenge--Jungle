import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import type { DataSource } from 'typeorm';
import { makeDataSource } from '../src/db';
import { down, up } from '../src/migration';
import { sha256, WagerService } from '../src/service';
import type { WagerInput } from '../src/service';
import { DeleteMessageCommand, GetQueueAttributesCommand, ReceiveMessageCommand, SendMessageCommand } from '@aws-sdk/client-sqs';
import { sqsClient, Workers } from '../src/worker';
import { metrics } from '../src/metrics';

const integration = process.env.RUN_INTEGRATION === '1' ? describe : describe.skip;
let ds: DataSource;
let service: WagerService;
const providerId = 'integration';
function input(walletId: string, playerId: string, externalTransactionId: string, kind: WagerInput['kind'], amount: string, referenceExternalTransactionId?: string): WagerInput {
  return { providerId, externalTransactionId, playerId, walletId, roundId: 'round-1', gameId: 'game-1', kind,
    money: { amount, currency: 'BRL' }, ...(referenceExternalTransactionId ? { referenceExternalTransactionId } : {}) };
}
integration('PostgreSQL financial invariants', () => {
  beforeAll(async () => {
    if (!process.env.DATABASE_URL?.endsWith('/wager_test')) throw Error('Set DATABASE_URL to the dedicated wager_test database');
    ds = await makeDataSource().initialize();
    const rows: { exists: string | null }[] = await ds.query("SELECT to_regclass('public.wallets')::text AS exists");
    if (!rows[0]?.exists) await ds.query(up);
    service = new WagerService(ds);
    const sqs = sqsClient();
    try {
      for (const name of ['wager-transactions.fifo', 'wager-transactions-dlq.fifo', 'wager-events.fifo']) {
        await waitUntil(async () => {
          try {
            await sqs.send(new GetQueueAttributesCommand({ QueueUrl: `http://localhost:4566/000000000000/${name}`, AttributeNames: ['QueueArn'] }));
            return true;
          } catch { return false; }
        }, 30000);
      }
    } finally { sqs.destroy(); }
  });
  afterAll(async () => { if (ds?.isInitialized) await ds.destroy(); });

  test('50 parallel duplicates debit once, preserve original balance, and reject a changed payload', async () => {
    const player = randomUUID();
    const wallet = await service.createWallet(player, { amount: '100.00', currency: 'BRL' });
    const wager = input(wallet.id, player, randomUUID(), 'BET', '30.00');
    const responses = await Promise.all(Array.from({ length: 50 }, () => service.process(wager, `key:${wager.externalTransactionId}`)));
    expect(new Set(responses.map(result => result.transactionId)).size).toBe(1);
    expect(responses.filter(result => !result.idempotentReplay)).toHaveLength(1);
    expect(responses.every(result => result.balance.amount === '70.00')).toBe(true);
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('70.00');
    const ledger: any = await service.ledger(wallet.id);
    expect(ledger.entries.filter((entry: any) => entry.direction === 'DEBIT')).toHaveLength(1);
    expect((await service.reconcile(wallet.id)).consistent).toBe(true);
    await expect(service.process({ ...wager, money: { amount: '31.00', currency: 'BRL' } }, `key:${wager.externalTransactionId}`)).rejects.toMatchObject({ code: 'IDEMPOTENCY_CONFLICT' });
  });

  test('one inbox message ID cannot process two different wallets concurrently', async () => {
    const playerA = randomUUID(), playerB = randomUUID();
    const walletA = await service.createWallet(playerA, { amount: '100.00', currency: 'BRL' });
    const walletB = await service.createWallet(playerB, { amount: '100.00', currency: 'BRL' });
    const transactionA = randomUUID(), transactionB = randomUUID(), messageId = randomUUID();
    const dataA = input(walletA.id, playerA, transactionA, 'BET', '10.00');
    const dataB = input(walletB.id, playerB, transactionB, 'BET', '10.00');
    const results = await Promise.allSettled([
      service.process(dataA, `key:${transactionA}`, { consumerName: 'wagering', messageId, payloadHash: sha256(dataA) }),
      service.process(dataB, `key:${transactionB}`, { consumerName: 'wagering', messageId, payloadHash: sha256(dataB) }),
    ]);
    expect(results.filter(result => result.status === 'fulfilled')).toHaveLength(1);
    expect(results.filter(result => result.status === 'rejected')).toHaveLength(1);
    expect([(await service.getWallet(walletA.id)).balance.amount, (await service.getWallet(walletB.id)).balance.amount].sort()).toEqual(['100.00', '90.00']);
    expect((await service.reconcile(walletA.id)).consistent).toBe(true);
    expect((await service.reconcile(walletB.id)).consistent).toBe(true);
  });

  test('two simultaneous 80.00 bets cannot overdraw a 100.00 wallet', async () => {
    const player = randomUUID();
    const wallet = await service.createWallet(player, { amount: '100.00', currency: 'BRL' });
    const ids = [randomUUID(), randomUUID()];
    const results = await Promise.all(ids.map(id => service.process(input(wallet.id, player, id, 'BET', '80.00'), `key:${id}`)));
    expect(results.map(result => result.status).sort()).toEqual(['PROCESSED', 'REJECTED']);
    expect(results.find(result => result.status === 'REJECTED')?.failureCode).toBe('INSUFFICIENT_FUNDS');
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('20.00');
    expect((await service.reconcile(wallet.id)).consistent).toBe(true);
  });

  test('out of order refund is retried and a second refund is rejected', async () => {
    const player = randomUUID();
    const wallet = await service.createWallet(player, { amount: '100.00', currency: 'BRL' });
    const betId = randomUUID(), refundId = randomUUID();
    const refund = await service.process(input(wallet.id, player, refundId, 'REFUND', '20.00', betId), `key:${refundId}`);
    expect(refund.status).toBe('PENDING_REFERENCE');
    await service.process(input(wallet.id, player, betId, 'BET', '20.00'), `key:${betId}`);
    await ds.query(`UPDATE wager_transactions SET next_attempt_at=now() WHERE id=$1`, [refund.transactionId]);
    await service.retryPending();
    expect((await service.getTransaction(refund.transactionId)).status).toBe('PROCESSED');
    const secondId = randomUUID();
    const second = await service.process(input(wallet.id, player, secondId, 'REFUND', '20.00', betId), `key:${secondId}`);
    expect(second.failureCode).toBe('ALREADY_REVERSED');
    expect((await service.reconcile(wallet.id)).consistent).toBe(true);
  });

  test('WIN, LOSS and ROLLBACK preserve the ledger and wallet version rules', async () => {
    const player = randomUUID();
    const wallet = await service.createWallet(player, { amount: '100.00', currency: 'BRL' });
    const betId = randomUUID(), winId = randomUUID(), lossId = randomUUID();
    expect((await service.process(input(wallet.id, player, betId, 'BET', '20.00'), `key:${betId}`)).balance.amount).toBe('80.00');
    expect((await service.process(input(wallet.id, player, winId, 'WIN', '50.00', betId), `key:${winId}`)).balance.amount).toBe('130.00');
    expect((await service.process(input(wallet.id, player, lossId, 'LOSS', '1.00'), `key:${lossId}`)).balance.amount).toBe('130.00');
    expect((await service.getWallet(wallet.id)).version).toBe(3);
    const rollbackWin = randomUUID(), rollbackBet = randomUUID();
    expect((await service.process(input(wallet.id, player, rollbackWin, 'ROLLBACK', '50.00', winId), `key:${rollbackWin}`)).balance.amount).toBe('80.00');
    expect((await service.process(input(wallet.id, player, rollbackBet, 'ROLLBACK', '20.00', betId), `key:${rollbackBet}`)).balance.amount).toBe('100.00');
    expect((await service.getWallet(wallet.id)).version).toBe(5);
    expect((await service.ledger(wallet.id)).entries).toHaveLength(5);
    expect((await service.reconcile(wallet.id)).consistent).toBe(true);
  });

  test('ROLLBACK before BET waits, and reversal without funds has a distinct code', async () => {
    const player = randomUUID();
    const wallet = await service.createWallet(player, { amount: '100.00', currency: 'BRL' });
    const betId = randomUUID(), rollbackId = randomUUID();
    const pending = await service.process(input(wallet.id, player, rollbackId, 'ROLLBACK', '20.00', betId), `key:${rollbackId}`);
    expect(pending.status).toBe('PENDING_REFERENCE');
    await service.process(input(wallet.id, player, betId, 'BET', '20.00'), `key:${betId}`);
    await ds.query('UPDATE wager_transactions SET next_attempt_at=now() WHERE id=$1', [pending.transactionId]);
    await service.retryPending();
    expect((await service.getTransaction(pending.transactionId)).status).toBe('PROCESSED');
    const secondBet = randomUUID(), refund = randomUUID(), spend = randomUUID(), reverseRefund = randomUUID();
    await service.process(input(wallet.id, player, secondBet, 'BET', '20.00'), `key:${secondBet}`);
    await service.process(input(wallet.id, player, refund, 'REFUND', '20.00', secondBet), `key:${refund}`);
    await service.process(input(wallet.id, player, spend, 'BET', '90.00'), `key:${spend}`);
    const result = await service.process(input(wallet.id, player, reverseRefund, 'ROLLBACK', '20.00', refund), `key:${reverseRefund}`);
    expect(result.status).toBe('REJECTED');
    expect(result.failureCode).toBe('REVERSAL_INSUFFICIENT_FUNDS');
    expect((await service.reconcile(wallet.id)).consistent).toBe(true);
  });

  test('wallets with different IDs do not wait on each other', async () => {
    const playerA = randomUUID(), playerB = randomUUID();
    const left = await service.createWallet(playerA, { amount: '100.00', currency: 'BRL' });
    const right = await service.createWallet(playerB, { amount: '100.00', currency: 'BRL' });
    await ds.transaction(async manager => {
      await manager.query('SELECT id FROM wallets WHERE id=$1 FOR UPDATE', [left.id]);
      const id = randomUUID();
      const result = await Promise.race([
        service.process(input(right.id, playerB, id, 'BET', '10.00'), `key:${id}`),
        new Promise<never>((_, reject) => setTimeout(() => reject(Error('Unrelated wallet blocked')), 2000)),
      ]);
      expect(result.status).toBe('PROCESSED');
    });
    expect((await service.reconcile(right.id)).consistent).toBe(true);
  });

  test('an outbox failure rolls back wallet, wager, ledger and inbox together', async () => {
    const player = randomUUID();
    const wallet = await service.createWallet(player, { amount: '100.00', currency: 'BRL' });
    const transactionId = randomUUID(), messageId = randomUUID();
    const data = input(wallet.id, player, transactionId, 'BET', '10.00');
    await ds.query('DROP TRIGGER IF EXISTS fail_outbox_test_trigger ON outbox_messages');
    await ds.query(`CREATE OR REPLACE FUNCTION fail_outbox_for_test() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN IF NEW.aggregate_id='${wallet.id}'::uuid THEN RAISE EXCEPTION 'injected outbox failure'; END IF; RETURN NEW; END $$`);
    await ds.query('CREATE TRIGGER fail_outbox_test_trigger BEFORE INSERT ON outbox_messages FOR EACH ROW EXECUTE FUNCTION fail_outbox_for_test()');
    try {
      await expect(service.process(data, `key:${transactionId}`, { consumerName: 'wagering', messageId, payloadHash: sha256(data) })).rejects.toThrow();
    } finally {
      await ds.query('DROP TRIGGER fail_outbox_test_trigger ON outbox_messages');
      await ds.query('DROP FUNCTION fail_outbox_for_test()');
    }
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('100.00');
    const rows: { wagers: string; debits: string; inbox: string }[] = await ds.query(`SELECT
      (SELECT COUNT(*)::text FROM wager_transactions WHERE external_transaction_id=$1) AS wagers,
      (SELECT COUNT(*)::text FROM wallet_ledger WHERE wallet_id=$2 AND direction='DEBIT') AS debits,
      (SELECT COUNT(*)::text FROM inbox_messages WHERE message_id=$3) AS inbox`, [transactionId, wallet.id, messageId]);
    expect(rows[0]).toEqual({ wagers: '0', debits: '0', inbox: '0' });
    expect((await service.reconcile(wallet.id)).consistent).toBe(true);
  });

  test('schema rejects ledger mutation and a negative wallet balance', async () => {
    const player = randomUUID();
    const wallet = await service.createWallet(player, { amount: '10.00', currency: 'BRL' });
    await expect(ds.query('UPDATE wallets SET balance=-1 WHERE id=$1', [wallet.id])).rejects.toThrow();
    await expect(ds.query('DELETE FROM wallet_ledger WHERE wallet_id=$1', [wallet.id])).rejects.toThrow();
  });

  test('migration creates constraints and reverses cleanly in an isolated schema', async () => {
    const schema = `migration_${randomUUID().replaceAll('-', '')}`;
    await ds.query(`CREATE SCHEMA ${schema}`);
    const runner = ds.createQueryRunner();
    await runner.connect();
    try {
      await runner.query(`SET search_path TO ${schema}, public`);
      await runner.query(up);
      const created: { table_name: string | null }[] = await runner.query(`SELECT to_regclass('${schema}.wallets')::text AS table_name`);
      expect(created[0]?.table_name).not.toBeNull();
      await expect(runner.query(`INSERT INTO wallets(id,player_id,currency,balance) VALUES ($1,$2,'BRL',-1)`,
        [randomUUID(), randomUUID()])).rejects.toThrow();
      await runner.query(down);
      const removed: { table_name: string | null }[] = await runner.query(`SELECT to_regclass('${schema}.wallets')::text AS table_name`);
      expect(removed[0]?.table_name).toBeNull();
    } finally {
      await runner.release();
      await ds.query(`DROP SCHEMA ${schema} CASCADE`);
    }
  });

  test('reconciliation reports and counts a balance mismatch without changing financial records', async () => {
    const wallet = await service.createWallet(randomUUID(), { amount: '10.00', currency: 'BRL' });
    const before = Number(metrics().match(/wager_reconciliation_mismatches_total\s+(\d+)/)?.[1] ?? '0');
    await ds.query('UPDATE wallets SET balance=11.00 WHERE id=$1', [wallet.id]);
    try {
      const result = await service.reconcile(wallet.id);
      expect(result).toMatchObject({ consistent: false, storedBalance: { amount: '11.00', currency: 'BRL' },
        calculatedBalance: { amount: '10.00', currency: 'BRL' }, difference: { amount: '1.00', currency: 'BRL' } });
      const after = Number(metrics().match(/wager_reconciliation_mismatches_total\s+(\d+)/)?.[1] ?? '0');
      expect(after).toBe(before + 1);
      expect((await service.getWallet(wallet.id)).balance.amount).toBe('11.00');
      expect((await service.ledger(wallet.id)).entries).toHaveLength(1);
    } finally {
      await ds.query('UPDATE wallets SET balance=10.00 WHERE id=$1', [wallet.id]);
    }
    expect((await service.reconcile(wallet.id)).consistent).toBe(true);
    expect(Number(metrics().match(/wager_reconciliation_mismatches_total\s+(\d+)/)?.[1] ?? '0')).toBe(before + 1);
  });

  test('three real processes serialize bets on one wallet', async () => {
    const player = randomUUID();
    const wallet = await service.createWallet(player, { amount: '100.00', currency: 'BRL' });
    const children = Array.from({ length: 3 }, () => Bun.spawn([process.execPath, 'test/concurrency-child.ts', wallet.id, player, randomUUID()],
      { cwd: process.cwd(), env: { ...process.env, DATABASE_URL: process.env.DATABASE_URL! }, stdout: 'pipe', stderr: 'pipe' }));
    const results = await Promise.all(children.map(async child => {
      const output = await new Response(child.stdout).text();
      const error = await new Response(child.stderr).text();
      expect(await child.exited).toBe(0);
      expect(error).toBe('');
      return JSON.parse(output);
    }));
    expect(results.filter(result => result.status === 'PROCESSED')).toHaveLength(1);
    expect(results.filter(result => result.status === 'REJECTED')).toHaveLength(2);
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('20.00');
    expect((await service.reconcile(wallet.id)).consistent).toBe(true);
  });

  test('two publishers claim different outbox rows and publish all events', async () => {
    const player = randomUUID();
    const wallet = await service.createWallet(player, { amount: '100.00', currency: 'BRL' });
    const transactionId = randomUUID();
    await service.process(input(wallet.id, player, transactionId, 'BET', '10.00'), `key:${transactionId}`);
    const rows: { id: string }[] = await ds.query('SELECT id FROM outbox_messages WHERE aggregate_id=$1', [wallet.id]);
    expect(rows).toHaveLength(4);

    const first = ds.createQueryRunner(), second = ds.createQueryRunner();
    await first.connect(); await second.connect();
    try {
      await first.startTransaction(); await second.startTransaction();
      const claim = `SELECT id FROM outbox_messages WHERE aggregate_id=$1 AND published_at IS NULL
        ORDER BY next_attempt_at,id LIMIT 1 FOR UPDATE SKIP LOCKED`;
      const firstClaim: { id: string }[] = await first.query(claim, [wallet.id]);
      const secondClaim: { id: string }[] = await second.query(claim, [wallet.id]);
      expect(firstClaim).toHaveLength(1);
      expect(secondClaim).toHaveLength(1);
      expect(firstClaim[0]!.id).not.toBe(secondClaim[0]!.id);
    } finally {
      if (first.isTransactionActive) await first.rollbackTransaction();
      if (second.isTransactionActive) await second.rollbackTransaction();
      await first.release(); await second.release();
    }

    const sqs = sqsClient();
    const worker1 = new Workers(sqs, ds, service), worker2 = new Workers(sqs, ds, service);
    worker1.start(); worker2.start();
    try {
      await waitUntil(async () => {
        const status: { published: string }[] = await ds.query('SELECT COUNT(*)::text AS published FROM outbox_messages WHERE aggregate_id=$1 AND published_at IS NOT NULL', [wallet.id]);
        return status[0]?.published === '4';
      }, 30000);
      const published: { id: string; attempts: number }[] = await ds.query('SELECT id,attempts FROM outbox_messages WHERE aggregate_id=$1 AND published_at IS NOT NULL', [wallet.id]);
      expect(published).toHaveLength(4);
      expect(published.every(row => row.attempts === 0)).toBe(true);
      expect((await service.reconcile(wallet.id)).consistent).toBe(true);
    } finally { await Promise.all([worker1.stop(), worker2.stop()]); sqs.destroy(); }
  }, 40000);

  test('a transient database failure retries the SQS message and then commits once', async () => {
    const sqs = sqsClient();
    const requestQueue = process.env.SQS_REQUEST_QUEUE_URL ?? 'http://localhost:4566/000000000000/wager-transactions.fifo';
    const player = randomUUID();
    const wallet = await service.createWallet(player, { amount: '100.00', currency: 'BRL' });
    const externalTransactionId = randomUUID(), messageId = randomUUID();
    const retriesBefore = Number(metrics().match(/wager_retries_total\s+(\d+)/)?.[1] ?? '0');
    const worker = new Workers(sqs, ds, service);
    await ds.query(`CREATE OR REPLACE FUNCTION fail_outbox_transient_for_test() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN IF NEW.aggregate_id='${wallet.id}'::uuid THEN RAISE EXCEPTION 'temporary test failure'; END IF; RETURN NEW; END $$`);
    await ds.query('CREATE TRIGGER fail_outbox_transient_test_trigger BEFORE INSERT ON outbox_messages FOR EACH ROW EXECUTE FUNCTION fail_outbox_transient_for_test()');
    try {
      worker.start();
      const data = { ...input(wallet.id, player, externalTransactionId, 'BET', '10.00'), idempotencyKey: `key:${externalTransactionId}` };
      await sqs.send(new SendMessageCommand({ QueueUrl: requestQueue,
        MessageBody: JSON.stringify({ messageId, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(), data }),
        MessageGroupId: wallet.id, MessageDeduplicationId: randomUUID() }));
      await waitUntil(async () => Number(metrics().match(/wager_retries_total\s+(\d+)/)?.[1] ?? '0') > retriesBefore);
      await ds.query('DROP TRIGGER fail_outbox_transient_test_trigger ON outbox_messages');
      await ds.query('DROP FUNCTION fail_outbox_transient_for_test()');
      await waitUntil(async () => (await service.getProviderTransaction(providerId, externalTransactionId).catch(() => null))?.status === 'PROCESSED');
      expect((await service.getWallet(wallet.id)).balance.amount).toBe('90.00');
      const ledger: any = await service.ledger(wallet.id);
      expect(ledger.entries.filter((entry: any) => entry.direction === 'DEBIT')).toHaveLength(1);
      const inbox: { count: string }[] = await ds.query('SELECT COUNT(*)::text AS count FROM inbox_messages WHERE message_id=$1', [messageId]);
      expect(inbox[0]?.count).toBe('1');
      expect((await service.reconcile(wallet.id)).consistent).toBe(true);
    } finally {
      await ds.query('DROP TRIGGER IF EXISTS fail_outbox_transient_test_trigger ON outbox_messages');
      await ds.query('DROP FUNCTION IF EXISTS fail_outbox_transient_for_test()');
      await worker.stop(); sqs.destroy();
    }
  }, 30000);

  test('real SQS redelivery commits inbox, ledger and outbox once', async () => {
    const sqs = sqsClient();
    const requestQueue = process.env.SQS_REQUEST_QUEUE_URL ?? 'http://localhost:4566/000000000000/wager-transactions.fifo';
    const dlqQueue = process.env.SQS_DLQ_URL ?? 'http://localhost:4566/000000000000/wager-transactions-dlq.fifo';
    const player = randomUUID();
    const wallet = await service.createWallet(player, { amount: '100.00', currency: 'BRL' });
    const externalTransactionId = randomUUID(), messageId = randomUUID();
    const body = JSON.stringify({ messageId, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(),
      data: { ...input(wallet.id, player, externalTransactionId, 'BET', '10.00'), idempotencyKey: `key:${externalTransactionId}` } });
    const worker1 = new Workers(sqs, ds, service), worker2 = new Workers(sqs, ds, service);
    worker1.start(); worker2.start();
    try {
      await sqs.send(new SendMessageCommand({ QueueUrl: requestQueue, MessageBody: body, MessageGroupId: wallet.id, MessageDeduplicationId: randomUUID() }));
      await waitUntil(async () => (await service.getProviderTransaction(providerId, externalTransactionId).catch(() => null))?.status === 'PROCESSED');
      // A second delivery uses another SQS dedup ID but the same application message ID.
      await sqs.send(new SendMessageCommand({ QueueUrl: requestQueue, MessageBody: body, MessageGroupId: wallet.id, MessageDeduplicationId: randomUUID() }));
      await waitUntil(async () => {
        const rows: { count: string }[] = await ds.query('SELECT COUNT(*)::text AS count FROM inbox_messages WHERE message_id=$1', [messageId]);
        return rows[0]?.count === '1' && /wager_duplicates_total\s+[1-9]/.test(metrics());
      });
      expect((await service.getWallet(wallet.id)).balance.amount).toBe('90.00');
      const ledger: any = await service.ledger(wallet.id);
      expect(ledger.entries.filter((entry: any) => entry.direction === 'DEBIT')).toHaveLength(1);
      await waitUntil(async () => {
        const rows: { pending: string }[] = await ds.query('SELECT COUNT(*)::text AS pending FROM outbox_messages WHERE aggregate_id=$1 AND published_at IS NULL', [wallet.id]);
        return rows[0]?.pending === '0';
      });
      expect((await service.reconcile(wallet.id)).consistent).toBe(true);
      const badMessage = randomUUID();
      await sqs.send(new SendMessageCommand({ QueueUrl: requestQueue, MessageBody: JSON.stringify({ messageId: badMessage, type: 'INVALID' }),
        MessageGroupId: badMessage, MessageDeduplicationId: badMessage }));
      await waitUntil(async () => {
        const batch = await sqs.send(new ReceiveMessageCommand({ QueueUrl: dlqQueue, WaitTimeSeconds: 0, MaxNumberOfMessages: 10, VisibilityTimeout: 1 }));
        const found = (batch.Messages ?? []).some(message => message.Body?.includes(badMessage));
        await Promise.all((batch.Messages ?? []).map(message => sqs.send(new DeleteMessageCommand({ QueueUrl: dlqQueue, ReceiptHandle: message.ReceiptHandle! }))));
        return found;
      });
    } finally { await Promise.all([worker1.stop(), worker2.stop()]); sqs.destroy(); }
  }, 30000);

  test('a new worker recovers a committed message left without ack', async () => {
    const sqs = sqsClient();
    const requestQueue = process.env.SQS_REQUEST_QUEUE_URL ?? 'http://localhost:4566/000000000000/wager-transactions.fifo';
    const player = randomUUID();
    const wallet = await service.createWallet(player, { amount: '100.00', currency: 'BRL' });
    const externalTransactionId = randomUUID(), messageId = randomUUID();
    const data = { ...input(wallet.id, player, externalTransactionId, 'BET', '25.00'), idempotencyKey: `key:${externalTransactionId}` };
    // Simulate a worker crash immediately after the database commit, before SQS DeleteMessage.
    await service.process(data, data.idempotencyKey, { consumerName: 'wagering', messageId, payloadHash: sha256(data) });
    const duplicatesBefore = Number(metrics().match(/wager_duplicates_total\s+(\d+)/)?.[1] ?? '0');
    const restarted = new Workers(sqs, ds, service);
    restarted.start();
    try {
      await sqs.send(new SendMessageCommand({ QueueUrl: requestQueue,
        MessageBody: JSON.stringify({ messageId, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(), data }),
        MessageGroupId: wallet.id, MessageDeduplicationId: randomUUID() }));
      await waitUntil(async () => {
        const rows: { pending: string }[] = await ds.query('SELECT COUNT(*)::text AS pending FROM outbox_messages WHERE aggregate_id=$1 AND published_at IS NULL', [wallet.id]);
        const duplicatesNow = Number(metrics().match(/wager_duplicates_total\s+(\d+)/)?.[1] ?? '0');
        return rows[0]?.pending === '0' && duplicatesNow > duplicatesBefore;
      });
      expect((await service.getWallet(wallet.id)).balance.amount).toBe('75.00');
      const ledger: any = await service.ledger(wallet.id);
      expect(ledger.entries.filter((entry: any) => entry.direction === 'DEBIT')).toHaveLength(1);
      expect((await service.reconcile(wallet.id)).consistent).toBe(true);
    } finally { await restarted.stop(); sqs.destroy(); }
  }, 30000);

  test('a killed process after commit is recovered by another worker without a second debit', async () => {
    const sqs = sqsClient();
    const requestQueue = process.env.SQS_REQUEST_QUEUE_URL ?? 'http://localhost:4566/000000000000/wager-transactions.fifo';
    const player = randomUUID();
    const wallet = await service.createWallet(player, { amount: '100.00', currency: 'BRL' });
    const externalTransactionId = randomUUID(), messageId = randomUUID();
    const data = { ...input(wallet.id, player, externalTransactionId, 'BET', '25.00'), idempotencyKey: `key:${externalTransactionId}` };
    await sqs.send(new SendMessageCommand({ QueueUrl: requestQueue,
      MessageBody: JSON.stringify({ messageId, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(), data }),
      MessageGroupId: wallet.id, MessageDeduplicationId: randomUUID() }));
    const child = Bun.spawn([process.execPath, 'test/crash-after-commit-child.ts', messageId],
      { cwd: process.cwd(), env: { ...process.env, DATABASE_URL: process.env.DATABASE_URL! }, stdout: 'pipe', stderr: 'pipe' });
    try {
      await waitUntil(async () => {
        const rows: { count: string }[] = await ds.query('SELECT COUNT(*)::text AS count FROM inbox_messages WHERE message_id=$1', [messageId]);
        return rows[0]?.count === '1';
      });
      child.kill('SIGKILL');
      expect(await child.exited).not.toBe(0);
      const duplicatesBefore = Number(metrics().match(/wager_duplicates_total\s+(\d+)/)?.[1] ?? '0');
      const recovered = new Workers(sqs, ds, service);
      recovered.start();
      try {
        await waitUntil(async () => Number(metrics().match(/wager_duplicates_total\s+(\d+)/)?.[1] ?? '0') > duplicatesBefore);
        expect((await service.getWallet(wallet.id)).balance.amount).toBe('75.00');
        const ledger: any = await service.ledger(wallet.id);
        expect(ledger.entries.filter((entry: any) => entry.direction === 'DEBIT')).toHaveLength(1);
        expect((await service.reconcile(wallet.id)).consistent).toBe(true);
      } finally { await recovered.stop(); }
    } finally {
      child.kill('SIGKILL');
      sqs.destroy();
    }
  }, 30000);
});

async function waitUntil(check: () => Promise<boolean>, timeoutMs = 15000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  throw Error('Timed out waiting for async processing');
}
