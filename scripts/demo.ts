import { randomUUID } from 'node:crypto';

const base = process.env.API_URL ?? 'http://localhost:3000';
type Money = { amount: string; currency: string };
type Wallet = { id: string; balance: Money };
type Result = { transactionId: string; status: string; balance: Money; failureCode?: string; idempotentReplay: boolean };
type Ledger = { entries: { direction: string; money: Money }[] };
type Reconciliation = { consistent: boolean; storedBalance: Money; calculatedBalance: Money };
type ApiResponse<T> = { http: number; body: T };

async function request<T>(path: string, options?: RequestInit): Promise<ApiResponse<T>> {
  const response = await fetch(`${base}${path}`, options);
  return { http: response.status, body: await response.json() as T };
}

function expect(condition: boolean, description: string): void {
  if (!condition) throw new Error(`Demonstração falhou: ${description}`);
}

function wager(playerId: string, walletId: string, externalTransactionId: string, amount: string): Record<string, unknown> {
  return { providerId: 'demo-local', externalTransactionId, playerId, walletId,
    roundId: 'rodada-demo', gameId: 'jogo-demo', kind: 'BET', money: { amount, currency: 'BRL' } };
}

function submit(key: string, body: Record<string, unknown>): Promise<ApiResponse<Result>> {
  return request<Result>('/wagering/transactions', {
    method: 'POST', headers: { 'content-type': 'application/json', 'Idempotency-Key': key }, body: JSON.stringify(body),
  });
}

try {
  const ready = await request<{ status: string }>('/health/ready');
  expect(ready.http === 200 && ready.body.status === 'ok', 'API não está pronta');
  console.log('API pronta. Iniciando cenário com dados novos.\n');

  const playerId = randomUUID();
  const created = await request<Wallet>('/wallets', { method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ playerId, initialBalance: { amount: '100.00', currency: 'BRL' } }) });
  expect(created.http === 201 && !!created.body.id, 'criação da carteira');
  const walletId = created.body.id;
  console.log(`1. Carteira criada com R$ 100,00: ${walletId}`);

  const attempts = [randomUUID(), randomUUID()].map(id => ({ key: `demo-local:${id}`, body: wager(playerId, walletId, id, '80.00') }));
  const results = await Promise.all(attempts.map(attempt => submit(attempt.key, attempt.body)));
  const winnerIndex = results.findIndex(result => result.http === 200 && result.body.status === 'PROCESSED');
  const rejected = results.filter(result => result.http === 422 && result.body.status === 'REJECTED' && result.body.failureCode === 'INSUFFICIENT_FUNDS');
  expect(winnerIndex !== -1 && rejected.length === 1, 'duas apostas concorrentes de R$ 80 devem gerar uma aprovação e uma rejeição');
  console.log('2. Duas apostas de R$ 80,00 enviadas ao mesmo tempo: 1 aprovada, 1 rejeitada por saldo insuficiente.');

  const replay = await submit(attempts[winnerIndex]!.key, attempts[winnerIndex]!.body);
  expect(replay.http === 200 && replay.body.idempotentReplay && replay.body.transactionId === results[winnerIndex]!.body.transactionId,
    'reenvio deve devolver a mesma transação');
  console.log(`3. Reenvio da aposta aprovada: mesma transação ${replay.body.transactionId}, sem novo débito.`);

  const changed = { ...attempts[winnerIndex]!.body, money: { amount: '81.00', currency: 'BRL' } };
  const conflict = await submit(attempts[winnerIndex]!.key, changed);
  expect(conflict.http === 409, 'reutilização da chave com valor diferente deve retornar HTTP 409');
  console.log('4. Mesma chave com valor diferente: HTTP 409 (conflito de idempotência).');

  const current = await request<Wallet>(`/wallets/${walletId}`);
  const ledger = await request<Ledger>(`/wallets/${walletId}/ledger`);
  const reconciliation = await request<Reconciliation>(`/wallets/${walletId}/reconciliation`, { method: 'POST' });
  expect(current.http === 200 && current.body.balance.amount === '20.00', 'saldo final deve ser R$ 20,00');
  expect(ledger.http === 200 && ledger.body.entries.filter(entry => entry.direction === 'DEBIT').length === 1,
    'ledger deve conter apenas um débito');
  expect(reconciliation.http === 201 && reconciliation.body.consistent && reconciliation.body.calculatedBalance.amount === '20.00',
    'saldo e ledger devem conferir');
  console.log('5. Saldo final: R$ 20,00. Ledger: 1 crédito de abertura e 1 débito. Reconciliação: consistente.\n');
  console.log(`Carteira: ${base}/wallets/${walletId}`);
  console.log(`Ledger: ${base}/wallets/${walletId}/ledger`);
  console.log(`Transação aprovada: ${base}/wagering/transactions/${replay.body.transactionId}`);
} catch (error) {
  console.error(error);
  process.exitCode = 1;
}
