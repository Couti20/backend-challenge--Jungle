export type MoneyProps = { amount: string; currency: string };
export type WagerKind = 'BET' | 'WIN' | 'LOSS' | 'REFUND' | 'ROLLBACK';
export type Status = 'PENDING' | 'PENDING_REFERENCE' | 'PROCESSED' | 'REJECTED' | 'FAILED';
export type Direction = 'DEBIT' | 'CREDIT';
const ISO_CURRENCIES = new Set(Intl.supportedValuesOf('currency'));

export class DomainError extends Error {
  constructor(public readonly code: string, message: string) { super(message); }
}

export class Money {
  private constructor(private readonly cents: bigint, public readonly currency: string) {}

  static from(props: MoneyProps): Money {
    if (!props || typeof props.amount !== 'string' || typeof props.currency !== 'string' || !/^\d+(?:\.\d{1,2})?$/.test(props.amount) ||
      !/^[A-Z]{3}$/.test(props.currency) || !ISO_CURRENCIES.has(props.currency)) {
      throw new DomainError('INVALID_MONEY', 'Expected a nonnegative decimal string with up to two decimal places and an ISO currency');
    }
    const [whole, fraction = ''] = props.amount.split('.');
    const cents = BigInt(whole!) * 100n + BigInt(fraction.padEnd(2, '0') || '0');
    if (cents > 99999999999999999999n) throw new DomainError('INVALID_MONEY', 'Amount exceeds database precision');
    return new Money(cents, props.currency);
  }

  static signed(amount: string, currency: string): Money {
    if (amount.startsWith('-')) return Money.from({ amount: amount.slice(1), currency }).negate();
    return Money.from({ amount, currency });
  }

  static zero(currency: string): Money { return Money.from({ amount: '0.00', currency }); }
  add(other: Money): Money { this.assertCurrency(other); return new Money(this.cents + other.cents, this.currency); }
  subtract(other: Money): Money { this.assertCurrency(other); return new Money(this.cents - other.cents, this.currency); }
  negate(): Money { return new Money(-this.cents, this.currency); }
  isZero(): boolean { return this.cents === 0n; }
  isPositive(): boolean { return this.cents > 0n; }
  isNegative(): boolean { return this.cents < 0n; }
  isLessThan(other: Money): boolean { this.assertCurrency(other); return this.cents < other.cents; }
  equals(other: Money): boolean { this.assertCurrency(other); return this.cents === other.cents; }
  toJSON(): MoneyProps {
    const abs = this.cents < 0n ? -this.cents : this.cents;
    return { amount: `${this.cents < 0n ? '-' : ''}${abs / 100n}.${String(abs % 100n).padStart(2, '0')}`, currency: this.currency };
  }
  toString(): string { return this.toJSON().amount; }
  private assertCurrency(other: Money): void {
    if (other.currency !== this.currency) throw new DomainError('CURRENCY_MISMATCH', 'Currencies differ');
  }
}

export class Wallet {
  private constructor(public readonly id: string, public readonly playerId: string, public readonly currency: string, private _balance: Money, private _version: number) {}
  static open(id: string, playerId: string, opening: Money): Wallet {
    if (opening.isNegative()) throw new DomainError('NEGATIVE_BALANCE', 'Opening balance cannot be negative');
    return new Wallet(id, playerId, opening.currency, opening, 1);
  }
  static rehydrate(id: string, playerId: string, balance: Money, version: number): Wallet { return new Wallet(id, playerId, balance.currency, balance, version); }
  get balance(): Money { return this._balance; }
  get version(): number { return this._version; }
  apply(direction: Direction, money: Money, insufficientCode = 'INSUFFICIENT_FUNDS'): { before: Money; after: Money } {
    if (!money.isPositive()) throw new DomainError('INVALID_MONEY', 'Financial movement must be positive');
    const before = this._balance;
    const after = direction === 'CREDIT' ? before.add(money) : before.subtract(money);
    if (after.isNegative()) throw new DomainError(insufficientCode, 'Movement would make balance negative');
    this._balance = after;
    this._version++;
    return { before, after };
  }
}

export class WalletLedgerEntry {
  private constructor(public readonly id: string, public readonly walletId: string, public readonly transactionId: string, public readonly direction: Direction, public readonly money: Money, public readonly balanceBefore: Money, public readonly balanceAfter: Money) {}
  static create(id: string, walletId: string, transactionId: string, direction: Direction, money: Money, before: Money, after: Money): WalletLedgerEntry {
    const expected = direction === 'CREDIT' ? before.add(money) : before.subtract(money);
    if (!money.isPositive() || !expected.equals(after) || after.isNegative()) throw new DomainError('UNBALANCED_LEDGER', 'Ledger arithmetic is invalid');
    return new WalletLedgerEntry(id, walletId, transactionId, direction, money, before, after);
  }
  static rehydrate(id: string, walletId: string, transactionId: string, direction: Direction, money: Money, before: Money, after: Money): WalletLedgerEntry { return new WalletLedgerEntry(id, walletId, transactionId, direction, money, before, after); }
  isBalanced(): boolean { return (this.direction === 'CREDIT' ? this.balanceBefore.add(this.money) : this.balanceBefore.subtract(this.money)).equals(this.balanceAfter); }
}

export class WagerTransaction {
  private constructor(
    public readonly id: string,
    public readonly kind: WagerKind,
    public readonly money: Money,
    public readonly payloadHash: string,
    public readonly referenceExternalTransactionId: string | undefined,
    private _status: Status,
    private _referenceTransactionId?: string,
    private _failureCode?: string,
    private _processedAt?: Date,
  ) {}
  static create(id: string, kind: WagerKind, money: Money, payloadHash: string, referenceExternalTransactionId?: string): WagerTransaction {
    if ((kind === 'REFUND' || kind === 'ROLLBACK') && !referenceExternalTransactionId) throw new DomainError('INVALID_REFERENCE', 'Reference required');
    return new WagerTransaction(id, kind, money, payloadHash, referenceExternalTransactionId, 'PENDING');
  }
  static rehydrate(state: { id: string; kind: WagerKind; money: Money; payloadHash: string; referenceExternalTransactionId?: string;
    status: Status; referenceTransactionId?: string; failureCode?: string; processedAt?: Date }): WagerTransaction {
    return new WagerTransaction(state.id, state.kind, state.money, state.payloadHash, state.referenceExternalTransactionId,
      state.status, state.referenceTransactionId, state.failureCode, state.processedAt);
  }
  get status(): Status { return this._status; }
  get failureCode(): string | undefined { return this._failureCode; }
  get processedAt(): Date | undefined { return this._processedAt; }
  get referenceTransactionId(): string | undefined { return this._referenceTransactionId; }
  isTerminal(): boolean { return ['PROCESSED', 'REJECTED', 'FAILED'].includes(this._status); }
  affectsBalance(): boolean { return this.kind !== 'LOSS'; }
  requiresReference(): boolean { return this.kind === 'REFUND' || this.kind === 'ROLLBACK'; }
  matchesPayload(hash: string): boolean { return this.payloadHash === hash; }
  ledgerDirectionFor(reference?: WagerTransaction): Direction | null { return movementFor(this.kind, reference?.kind); }
  markPendingReference(): void { this.assertActive(); this._status = 'PENDING_REFERENCE'; }
  markProcessed(referenceTransactionId: string | undefined, at: Date): void {
    this.assertActive(); this._status = 'PROCESSED'; this._referenceTransactionId = referenceTransactionId; this._processedAt = at;
  }
  reject(code: string): void { this.assertActive(); this._status = 'REJECTED'; this._failureCode = code; this._processedAt = new Date(); }
  fail(code: string): void { this.assertActive(); this._status = 'FAILED'; this._failureCode = code; this._processedAt = new Date(); }
  private assertActive(): void { if (this.isTerminal()) throw new DomainError('INVALID_TRANSACTION_STATE', 'Terminal transaction cannot change state'); }
}

export function movementFor(kind: WagerKind, referenceKind?: string): Direction | null {
  switch (kind) {
    case 'BET': return 'DEBIT';
    case 'WIN': case 'REFUND': return 'CREDIT';
    case 'LOSS': return null;
    case 'ROLLBACK': return referenceKind === 'BET' ? 'CREDIT' : 'DEBIT';
  }
}
