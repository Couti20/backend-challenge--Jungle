# Arquitetura

## Fluxo transacional

API e SQS chamam `WagerService.process`. Uma transação PostgreSQL bloqueia a linha da wallet com `SELECT ... FOR UPDATE`; em seguida consulta as chaves de idempotência, aplica a operação e confirma juntos: transação de aposta, saldo, ledger, inbox (SQS) e outbox. A fila FIFO ajuda na eficiência, mas as garantias vêm das constraints e dos locks por wallet. Carteiras diferentes não se bloqueiam. O saldo é alterado uma vez dentro do mesmo commit do lançamento. A versão começa em 1 e só sobe se o saldo mudar.

O domínio usa centavos em `bigint`; contratos e banco usam strings decimais e `NUMERIC(20,2)`. Não há arredondamento implícito: mais de duas casas são rejeitadas. `Money.from` valida a entrada não negativa, `Money.signed` serve apenas à reconciliação. TypeORM executa as transações e mantém o mapeamento das entidades; SQL explícito é usado nos pontos em que `FOR UPDATE`, `SKIP LOCKED`, constraints e precisão monetária precisam ficar evidentes. As migrations são versionadas e reversíveis (`bun run migrate` / `bun run migrate:down`); um advisory lock serializa migradores concorrentes na inicialização de múltiplas instâncias.

## Idempotência e efeitos

`idempotency_key` é globalmente única e `(provider_id, external_transaction_id)` também. O hash é SHA-256 de JSON canônico do subconjunto de negócio, com chaves ordenadas recursivamente e valor monetário normalizado; header, `messageId` e metadados não entram. Repetição idêntica devolve ID, status e saldo registrados no processamento original com `idempotentReplay: true`; divergência retorna `409`. `inbox_messages` tem PK `(consumer_name, message_id)` e hash do conteúdo da mensagem. O inbox é gravado no mesmo commit do efeito financeiro. Um crash depois do commit e antes do ack causa redelivery, que vira replay sem novo lançamento.

`wallet_ledger` é append only por trigger, tem uma linha por transação financeira e `CHECK` da aritmética. `wallets.balance >= 0`, unicidade de carteira por jogador/moeda e unicidade de reversão processada são constraints. Reconciliação soma o ledger sem corrigir divergências automaticamente. Cada divergência detectada retorna `consistent: false`, gera um log e incrementa `wager_reconciliation_mismatches_total` em `/metrics`.

## Regras de aposta

| Tipo | Efeito | Referência |
| --- | --- | --- |
| BET | débito, exige saldo | nenhuma |
| WIN | crédito | BET opcional da mesma rodada |
| LOSS | nenhum | nenhuma |
| REFUND | crédito | BET processada, valor igual |
| ROLLBACK | inverso | BET, WIN ou REFUND processada, valor igual |

Uma referência deve coincidir em provider, wallet, player, moeda e rodada. Um mesmo tipo de reversão pode ser processado só uma vez por referência. A operação com referência ainda ausente fica em `PENDING_REFERENCE`; worker reconsulta até oito tentativas, com backoff de 1 a 60 segundos, então rejeita. O efeito da reversão que deixaria saldo negativo é rejeitado com código específico. Rejeições não escrevem ledger nem mudam saldo.

Estados: `PENDING -> PROCESSED | REJECTED | PENDING_REFERENCE`; `PENDING_REFERENCE -> PENDING_REFERENCE | PROCESSED | REJECTED`. Estados terminais nunca são reprocessados. `FAILED` fica reservado para erro permanente auditável; falhas transitórias da infraestrutura deixam a transação SQL sem commit e são repetidas pela origem.

## Mensageria

O consumidor apaga uma mensagem SQS somente depois do commit. Erros de validação ou conflito vão à DLQ; falhas transitórias alteram a visibilidade com backoff e vão à DLQ após cinco recebimentos. No `SIGTERM`, o processo aguarda os lotes em andamento; mensagens sem ack retornam após a visibilidade vencer. O worker de referências pendentes é seguro em múltiplas instâncias porque bloqueia a wallet e relê o estado antes de aplicar.

O publisher usa `FOR UPDATE SKIP LOCKED` para reivindicar uma linha da outbox. Publica após o commit financeiro e marca `published_at` só depois do sucesso. Se cair nesse intervalo, pode publicar de novo; `eventId` permanece igual, de modo que o consumidor externo deve deduplicar por `eventId`. Os eventos são JSON versionado, com valores monetários em strings. Outbox com falha faz retry exponencial; backlog persiste durante indisponibilidade do SQS.

## HTTP e operação

`400`: entrada inválida. `404`: entidade ausente. `409`: conflito de carteira/idempotência. `422`: regra de negócio rejeitada. `202`: referência pendente. `503`: infraestrutura indisponível. Os endpoints de health são públicos. A autenticação foi deliberadamente deixada como `ProviderIdentityGuard` no-op, conforme a opção expressa no enunciado; em produção ele verificaria JWT/OIDC emitido por um IdP externo, associaria subject a provider e rejeitaria mensagens cujo provider não fosse autorizado.

Logs são JSON e incluem IDs para correlação, sem payload financeiro completo. `/metrics` expõe contadores, buckets de latência e espera por lock, e idade da mensagem mais antiga na outbox. São métricas locais e precisam ser agregadas entre réplicas na implantação. Eventos publicados podem chegar fora de ordem se dois publishers reivindicarem linhas distintas da mesma wallet; consumidores devem usar ID/versão de evento, e ordenação estrita por wallet exigiria uma etapa adicional de sequenciamento. Não há mecanismo de autenticação ativo nesta entrega.
