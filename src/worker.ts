import { ChangeMessageVisibilityCommand, DeleteMessageCommand, GetQueueAttributesCommand, ReceiveMessageCommand, SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { DataSource } from 'typeorm';
import { count, observe } from './metrics';
import { DomainError } from './domain';
import { sha256, WagerService } from './service';
import type { WagerInput } from './service';

const requestQueue = process.env.SQS_REQUEST_QUEUE_URL ?? 'http://localhost:4566/000000000000/wager-transactions.fifo';
const dlqQueue = process.env.SQS_DLQ_URL ?? 'http://localhost:4566/000000000000/wager-transactions-dlq.fifo';
const eventsQueue = process.env.SQS_EVENTS_QUEUE_URL ?? 'http://localhost:4566/000000000000/wager-events.fifo';
const pause = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const log = (level: string, event: string, fields: Record<string, unknown> = {}) => console.log(JSON.stringify({ level, event, ...fields }));
const errorCode = (error: unknown): string => error instanceof DomainError ? error.code
  : error && typeof error === 'object' ? String((error as any).driverError?.code ?? (error as any).code ?? (error as any).name ?? 'UNKNOWN') : 'UNKNOWN';

export function sqsClient(): SQSClient {
  return new SQSClient({ region: process.env.AWS_REGION ?? 'us-east-1', endpoint: process.env.SQS_ENDPOINT ?? 'http://localhost:4566',
    credentials: { accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test', secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test' } });
}

export class Workers {
  private running = false;
  private pending = new Set<Promise<unknown>>();
  constructor(private readonly sqs: SQSClient, private readonly ds: DataSource, private readonly service: WagerService) {}
  start(): void {
    this.running = true;
    this.track(this.consume());
    this.track(this.publish());
    this.track(this.retryReferences());
  }
  async stop(): Promise<void> {
    this.running = false;
    await Promise.allSettled([...this.pending]);
  }
  private track(promise: Promise<unknown>): void { this.pending.add(promise); void promise.finally(() => this.pending.delete(promise)); }

  private async consume(): Promise<void> {
    while (this.running) {
      try {
        const batch = await this.sqs.send(new ReceiveMessageCommand({ QueueUrl: requestQueue, MaxNumberOfMessages: 10,
          WaitTimeSeconds: 5, VisibilityTimeout: 30, MessageSystemAttributeNames: ['ApproximateReceiveCount'] }));
        await Promise.all((batch.Messages ?? []).map(message => this.consumeOne(message.Body ?? '', message.ReceiptHandle!,
          Number(message.Attributes?.ApproximateReceiveCount ?? '1'))));
      } catch (error) { log('error', 'sqs_consume_error', { errorCode: errorCode(error) }); await pause(1000); }
    }
  }
  private async consumeOne(body: string, receipt: string, receiveCount: number): Promise<void> {
    const started = performance.now();
    let envelope: { messageId: string; type: string; data: WagerInput & { idempotencyKey: string } } | undefined;
    let committed = false;
    try {
      envelope = JSON.parse(body);
      if (!envelope || envelope.type !== 'WagerTransactionRequested' || typeof envelope.messageId !== 'string' || !envelope.messageId || !envelope.data?.idempotencyKey)
        throw new DomainError('INVALID_MESSAGE', 'Invalid envelope');
      const { idempotencyKey, ...data } = envelope.data;
      const result = await this.service.process(data, idempotencyKey, { consumerName: 'wagering', messageId: envelope.messageId, payloadHash: sha256(envelope.data) });
      committed = true;
      count('wager_transactions_total', { status: result.status });
      observe('wager_processing_latency_ms', performance.now() - started);
      if (result.idempotentReplay) count('wager_duplicates_total');
      try { await this.sqs.send(new DeleteMessageCommand({ QueueUrl: requestQueue, ReceiptHandle: receipt })); }
      catch (error) { log('warn', 'sqs_ack_retry_on_redelivery', { messageId: envelope.messageId, errorCode: errorCode(error) }); return; }
      log('info', 'sqs_processed', { messageId: envelope.messageId, correlationId: result.transactionId, transactionId: result.transactionId,
        walletId: data.walletId, providerId: data.providerId, status: result.status });
    } catch (error) {
      if (committed) { log('warn', 'sqs_post_commit_error', { messageId: envelope?.messageId, errorCode: errorCode(error) }); return; }
      const permanent = error instanceof DomainError && ['INVALID_MESSAGE','INVALID_PAYLOAD','INVALID_MONEY','IDEMPOTENCY_CONFLICT','INBOX_CONFLICT','WALLET_NOT_FOUND','PLAYER_MISMATCH','CURRENCY_MISMATCH'].includes(error.code);
      if (permanent || receiveCount >= 5) {
        try {
          await this.sqs.send(new SendMessageCommand({ QueueUrl: dlqQueue, MessageBody: body,
            MessageGroupId: envelope?.data?.walletId ?? 'invalid', MessageDeduplicationId: `${envelope?.messageId ?? sha256(body)}-${receiveCount}` }));
          await this.sqs.send(new DeleteMessageCommand({ QueueUrl: requestQueue, ReceiptHandle: receipt }));
          count('wager_dlq_total');
          log('error', 'sqs_dlq', { messageId: envelope?.messageId, walletId: envelope?.data?.walletId, providerId: envelope?.data?.providerId,
            errorCode: errorCode(error) });
        } catch (dlqError) { log('error', 'sqs_dlq_publish_failed', { errorCode: errorCode(dlqError) }); }
      } else {
        count('wager_retries_total');
        const seconds = Math.min(2 ** receiveCount, 30);
        await this.sqs.send(new ChangeMessageVisibilityCommand({ QueueUrl: requestQueue, ReceiptHandle: receipt, VisibilityTimeout: seconds }));
        log('warn', 'sqs_retry', { messageId: envelope?.messageId, walletId: envelope?.data?.walletId, providerId: envelope?.data?.providerId,
          errorCode: errorCode(error), receiveCount });
      }
    }
  }
  private async publish(): Promise<void> {
    while (this.running) {
      try {
        let sent = 0;
        for (let i = 0; i < 20 && this.running; i++) {
          const didSend = await this.ds.transaction(async manager => {
            const rows: Record<string, any>[] = await manager.query(`SELECT * FROM outbox_messages WHERE published_at IS NULL AND next_attempt_at<=now()
              ORDER BY next_attempt_at,id LIMIT 1 FOR UPDATE SKIP LOCKED`);
            const row = rows[0];
            if (!row) return false;
            try {
              await this.sqs.send(new SendMessageCommand({ QueueUrl: eventsQueue, MessageBody: JSON.stringify(row.payload),
                MessageGroupId: row.aggregate_id, MessageDeduplicationId: row.id }));
              await manager.query('UPDATE outbox_messages SET published_at=now() WHERE id=$1', [row.id]);
            } catch (error) {
              const attempts = row.attempts + 1;
              await manager.query(`UPDATE outbox_messages SET attempts=$2,next_attempt_at=now()+($3::text || ' seconds')::interval WHERE id=$1`,
                [row.id, attempts, Math.min(2 ** Math.min(attempts, 8), 300)]);
              count('wager_outbox_retries_total');
              log('warn', 'outbox_retry', { eventId: row.id, walletId: row.aggregate_id, errorCode: errorCode(error) });
            }
            return true;
          });
          if (!didSend) break;
          sent++;
        }
        if (!sent) await pause(500);
      } catch (error) { log('error', 'outbox_error', { errorCode: errorCode(error) }); await pause(1000); }
    }
  }
  private async retryReferences(): Promise<void> {
    while (this.running) {
      try { await this.service.retryPending(); }
      catch (error) { log('error', 'reference_retry_error', { errorCode: errorCode(error) }); }
      await pause(1000);
    }
  }
  async ready(): Promise<boolean> {
    try {
      await this.ds.query('SELECT 1');
      await this.sqs.send(new GetQueueAttributesCommand({ QueueUrl: requestQueue, AttributeNames: ['QueueArn'] }));
      await this.sqs.send(new GetQueueAttributesCommand({ QueueUrl: eventsQueue, AttributeNames: ['QueueArn'] }));
      return true;
    } catch { return false; }
  }
}
