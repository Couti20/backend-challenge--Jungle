# Distributed Wagering Processor

Implementação do [desafio Backend da Jungle Gaming](https://github.com/junglegaming/backend-challenge) com Bun 1.x, TypeScript estrito, NestJS, TypeORM, PostgreSQL e SQS via LocalStack.

## Subir localmente

```bash
docker compose up --build -d
curl http://localhost:3000/health/ready
```

O primeiro build baixa as imagens e instala as dependências. A aplicação executa a migration antes de iniciar. PostgreSQL fica na porta 5432, SQS na 4566 e a API na 3000. Para logs: `docker compose logs -f app`. Para encerrar: `docker compose down` (mantém os dados); `docker compose down -v` remove o volume de desenvolvimento.

É possível rodar a aplicação fora do Docker: `bun install`, `cp .env.example .env`, `docker compose up -d postgres localstack`, `bun run migrate` e `bun run start`.

## Demonstração

Com a API em execução, rode em outro terminal (com Bun 1.x instalado):

```bash
bun run demo
```

O comando cria uma carteira nova, envia duas apostas concorrentes, verifica idempotência e confere o saldo com o ledger.

## Exemplo

```bash
curl -X POST http://localhost:3000/wallets \
  -H 'Content-Type: application/json' \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"100.00","currency":"BRL"}}'
```

Use o `id` retornado:

```bash
curl -X POST http://localhost:3000/wagering/transactions \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:bet-1' \
  -d '{"providerId":"provider-a","externalTransactionId":"bet-1","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"COLE_O_ID_DA_WALLET","roundId":"round-1","gameId":"game-1","kind":"BET","money":{"amount":"80.00","currency":"BRL"}}'
```

Consultas: `GET /wallets/:walletId`, `GET /wallets/:walletId/ledger?limit=50&cursor=...`, `GET /wagering/transactions/:transactionId`, `GET /providers/:providerId/wagering/transactions/:externalTransactionId`. Reconciliação: `POST /wallets/:walletId/reconciliation`. Health: `/health/live` e `/health/ready`. Métricas de contagem em `/metrics`.

## SQS

Filas: `wager-transactions.fifo`, `wager-transactions-dlq.fifo` e `wager-events.fifo`. Envie o envelope do enunciado para a primeira fila, com `MessageGroupId` igual ao `walletId` e `MessageDeduplicationId` único. O consumidor usa o mesmo caso de uso da API; a deduplicação durável fica no PostgreSQL.

## Testes

```bash
bun run typecheck
bun test test/domain.test.ts
```

Para a suíte de integração, crie um banco isolado, execute a migration e use a variável de proteção:

```bash
docker compose stop app
docker compose up -d postgres localstack
docker compose exec -T postgres psql -U wager -d postgres -c 'CREATE DATABASE wager_test;'
DATABASE_URL=postgres://wager:wager@localhost:5432/wager_test RUN_INTEGRATION=1 bun test test/integration.test.ts
docker compose start app
```

O teste de integração exige PostgreSQL e LocalStack reais; não limpa nem modifica a base de desenvolvimento. A API fica parada durante a suíte para não consumir as mensagens de teste. Se o banco `wager_test` já existir, ignore o erro do comando `CREATE DATABASE`.

## Decisões

Veja [ARCHITECTURE.md](ARCHITECTURE.md) para invariantes, locks, idempotência, retry, códigos de falha e limitações.
