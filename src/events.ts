import { randomUUID } from 'node:crypto';

interface Props<T> { aggregateId: string; correlationId: string; data: T }
export abstract class IntegrationEvent<T> {
  abstract readonly eventType: string;
  abstract readonly version: number;
  readonly eventId = randomUUID();
  readonly occurredAt = new Date();
  readonly aggregateId: string;
  readonly correlationId: string;
  readonly data: Readonly<T>;
  protected constructor(props: Props<T>) {
    this.aggregateId = props.aggregateId;
    this.correlationId = props.correlationId;
    this.data = Object.freeze(props.data);
  }
  toJSON() { return { eventId: this.eventId, eventType: this.eventType, aggregateId: this.aggregateId,
    correlationId: this.correlationId, occurredAt: this.occurredAt.toISOString(), version: this.version, data: this.data }; }
}
type Data = Record<string, unknown>;
export class WagerTransactionProcessed extends IntegrationEvent<Data> {
  readonly eventType = 'WagerTransactionProcessed'; readonly version = 1;
  static from(walletId: string, transactionId: string, data: Data) { return new WagerTransactionProcessed({ aggregateId: walletId, correlationId: transactionId, data }); }
}
export class WagerTransactionRejected extends IntegrationEvent<Data> {
  readonly eventType = 'WagerTransactionRejected'; readonly version = 1;
  static from(walletId: string, transactionId: string, data: Data) { return new WagerTransactionRejected({ aggregateId: walletId, correlationId: transactionId, data }); }
}
export class WagerTransactionPendingReference extends IntegrationEvent<Data> {
  readonly eventType = 'WagerTransactionPendingReference'; readonly version = 1;
  static from(walletId: string, transactionId: string, data: Data) { return new WagerTransactionPendingReference({ aggregateId: walletId, correlationId: transactionId, data }); }
}
export class WalletBalanceChanged extends IntegrationEvent<Data> {
  readonly eventType = 'WalletBalanceChanged'; readonly version = 1;
  static from(walletId: string, transactionId: string, data: Data) { return new WalletBalanceChanged({ aggregateId: walletId, correlationId: transactionId, data }); }
}
