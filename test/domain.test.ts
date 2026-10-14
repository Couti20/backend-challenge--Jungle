import { describe, expect, test } from 'bun:test';
import { DomainError, Money, movementFor, Wallet, WalletLedgerEntry, WagerTransaction } from '../src/domain';
import { sha256 } from '../src/service';

describe('Money', () => {
  test('uses exact cents and serializes a fixed scale', () => {
    const sum = Money.from({ amount: '0.10', currency: 'BRL' }).add(Money.from({ amount: '0.20', currency: 'BRL' }));
    expect(sum.toJSON()).toEqual({ amount: '0.30', currency: 'BRL' });
    expect(sum.subtract(Money.from({ amount: '0.01', currency: 'BRL' })).toString()).toBe('0.29');
    expect(sum.negate().toString()).toBe('-0.30');
  });
  test.each(['', '-1.00', '1e3', 'NaN', 'Infinity', '1.001', ' 1.00', '1,00'])('rejects invalid input %s', value => {
    expect(() => Money.from({ amount: value, currency: 'BRL' })).toThrow(DomainError);
  });
  test('rejects mixed currencies', () => {
    expect(() => Money.zero('BRL').add(Money.zero('USD'))).toThrow(DomainError);
    expect(() => Money.zero('XYZ')).toThrow(DomainError);
  });
});

describe('Wallet and ledger', () => {
  test('debit never makes the wallet negative and changes version only on success', () => {
    const wallet = Wallet.open('wallet', 'player', Money.from({ amount: '100.00', currency: 'BRL' }));
    expect(() => wallet.apply('DEBIT', Money.from({ amount: '101.00', currency: 'BRL' }))).toThrow(DomainError);
    expect(wallet.balance.toString()).toBe('100.00');
    expect(wallet.version).toBe(1);
    const { before, after } = wallet.apply('DEBIT', Money.from({ amount: '80.00', currency: 'BRL' }));
    expect(after.toString()).toBe('20.00');
    expect(wallet.version).toBe(2);
    expect(WalletLedgerEntry.create('entry', wallet.id, 'tx', 'DEBIT', Money.from({ amount: '80.00', currency: 'BRL' }), before, after).isBalanced()).toBe(true);
    expect(() => WalletLedgerEntry.create('entry', wallet.id, 'tx', 'CREDIT', Money.from({ amount: '80.00', currency: 'BRL' }), before, after)).toThrow(DomainError);
  });
  test('business movements are explicit', () => {
    expect(movementFor('BET')).toBe('DEBIT');
    expect(movementFor('WIN')).toBe('CREDIT');
    expect(movementFor('LOSS')).toBeNull();
    expect(movementFor('REFUND')).toBe('CREDIT');
    expect(movementFor('ROLLBACK', 'BET')).toBe('CREDIT');
    expect(movementFor('ROLLBACK', 'WIN')).toBe('DEBIT');
  });
  test('canonical payload hashing is key order independent', () => {
    expect(sha256({ b: 2, a: { y: 2, x: 1 } })).toBe(sha256({ a: { x: 1, y: 2 }, b: 2 }));
  });
  test('terminal wager transitions cannot be changed', () => {
    const wager = WagerTransaction.create('tx', 'BET', Money.from({ amount: '1.00', currency: 'BRL' }), 'hash');
    wager.markProcessed(undefined, new Date());
    expect(wager.status).toBe('PROCESSED');
    expect(() => wager.reject('INSUFFICIENT_FUNDS')).toThrow(DomainError);
    expect(() => WagerTransaction.create('refund', 'REFUND', Money.from({ amount: '1.00', currency: 'BRL' }), 'hash')).toThrow(DomainError);
  });
});
