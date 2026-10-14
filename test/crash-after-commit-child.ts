import { ReceiveMessageCommand } from '@aws-sdk/client-sqs';
import { makeDataSource } from '../src/db';
import { sha256, WagerService } from '../src/service';
import { sqsClient } from '../src/worker';

const expectedMessageId = process.argv[2];
if (!expectedMessageId) throw Error('Expected message ID');
const requestQueue = process.env.SQS_REQUEST_QUEUE_URL ?? 'http://localhost:4566/000000000000/wager-transactions.fifo';
const ds = await makeDataSource().initialize();
const sqs = sqsClient();
const service = new WagerService(ds);
const deadline = Date.now() + 15000;
while (Date.now() < deadline) {
  const batch = await sqs.send(new ReceiveMessageCommand({ QueueUrl: requestQueue, WaitTimeSeconds: 1, VisibilityTimeout: 2 }));
  for (const message of batch.Messages ?? []) {
    const envelope = JSON.parse(message.Body ?? '{}');
    if (envelope.messageId !== expectedMessageId) continue;
    const { idempotencyKey, ...data } = envelope.data;
    await service.process(data, idempotencyKey, { consumerName: 'wagering', messageId: envelope.messageId, payloadHash: sha256(envelope.data) });
    // Stay alive until the parent kills this process. The SQS message is deliberately not acknowledged.
    await new Promise(() => { setInterval(() => {}, 1000); });
  }
}
throw Error('Expected SQS message was not received');
