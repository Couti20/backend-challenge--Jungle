import 'reflect-metadata';
import { Body, Catch, Controller, Get, Headers, HttpException, HttpStatus, Inject, Injectable, Module, Param, Post, Query, Res, UseGuards } from '@nestjs/common';
import type { ArgumentsHost, CanActivate, ExceptionFilter } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { DataSource } from 'typeorm';
import { DomainError } from './domain';
import { makeDataSource } from './db';
import { metrics, count, gauge, observe } from './metrics';
import { WagerService } from './service';
import type { WagerInput } from './service';
import { sqsClient, Workers } from './worker';

// Extension point: replace this with an OIDC provider identity guard when auth is enabled.
@Injectable()
class ProviderIdentityGuard implements CanActivate { canActivate(): boolean { return true; } }

@Catch()
class ApiErrors implements ExceptionFilter {
  catch(error: unknown, host: ArgumentsHost): void {
    const response = host.switchToHttp().getResponse();
    if (error instanceof DomainError) {
      const code = ['INVALID_PAYLOAD','INVALID_MONEY'].includes(error.code) ? 400
        : ['WALLET_NOT_FOUND','TRANSACTION_NOT_FOUND'].includes(error.code) ? 404
        : ['IDEMPOTENCY_CONFLICT','INBOX_CONFLICT','WALLET_EXISTS'].includes(error.code) ? 409
        : ['INSUFFICIENT_FUNDS','REVERSAL_INSUFFICIENT_FUNDS'].includes(error.code) ? 422 : 400;
      response.status(code).json({ code: error.code, message: error.message });
    } else if (error instanceof HttpException) {
      response.status(error.getStatus()).json(error.getResponse());
    } else {
      const errorCode = error && typeof error === 'object' ? String((error as any).driverError?.code ?? (error as any).code ?? (error as any).name ?? 'UNKNOWN') : 'UNKNOWN';
      console.error(JSON.stringify({ level: 'error', event: 'http_error', errorCode }));
      response.status(503).json({ code: 'INFRASTRUCTURE_UNAVAILABLE', message: 'Please retry' });
    }
  }
}

@Controller('wallets')
@UseGuards(ProviderIdentityGuard)
class WalletController {
  constructor(@Inject('SERVICE') private readonly service: WagerService) {}
  @Post() async create(@Body() body: { playerId: string; initialBalance: { amount: string; currency: string } }, @Res() res: any): Promise<void> {
    const value = await this.service.createWallet(body?.playerId, body?.initialBalance);
    res.status(201).json(value);
  }
  @Get(':walletId/ledger') ledger(@Param('walletId') id: string, @Query('cursor') cursor?: string, @Query('limit') limit?: string): Promise<unknown> {
    return this.service.ledger(id, cursor, limit === undefined ? 50 : Number(limit));
  }
  @Post(':walletId/reconciliation') reconcile(@Param('walletId') id: string): Promise<unknown> { return this.service.reconcile(id); }
  @Get(':walletId') get(@Param('walletId') id: string): Promise<unknown> { return this.service.getWallet(id); }
}

@Controller('wagering/transactions')
@UseGuards(ProviderIdentityGuard)
class WagerController {
  constructor(@Inject('SERVICE') private readonly service: WagerService) {}
  @Post() async create(@Body() body: WagerInput, @Headers('idempotency-key') key: string, @Res() res: any): Promise<void> {
    const started = performance.now();
    const result = await this.service.process(body, key);
    count('wager_transactions_total', { status: result.status });
    if (result.idempotentReplay) count('wager_duplicates_total');
    const elapsed = Math.round(performance.now() - started);
    observe('wager_processing_latency_ms', elapsed);
    console.log(JSON.stringify({ level: 'info', event: 'http_wager', correlationId: result.transactionId, transactionId: result.transactionId,
      walletId: body.walletId, providerId: body.providerId, status: result.status, latencyMs: elapsed }));
    res.status(result.status === 'PENDING_REFERENCE' ? 202 : result.status === 'REJECTED' ? 422 : 200).json(result);
  }
  @Get(':transactionId') get(@Param('transactionId') id: string): Promise<unknown> { return this.service.getTransaction(id); }
}

@Controller('providers/:providerId/wagering/transactions')
@UseGuards(ProviderIdentityGuard)
class ProviderTransactionController {
  constructor(@Inject('SERVICE') private readonly service: WagerService) {}
  @Get(':externalTransactionId') get(@Param('providerId') providerId: string, @Param('externalTransactionId') id: string): Promise<unknown> {
    return this.service.getProviderTransaction(providerId, id);
  }
}

@Controller()
class HealthController {
  constructor(@Inject('WORKERS') private readonly workers: Workers, @Inject('DS') private readonly ds: DataSource) {}
  @Get('health/live') live(): unknown { return { status: 'ok' }; }
  @Get('health/ready') async ready(@Res() res: any): Promise<void> {
    const ready = await this.workers.ready();
    res.status(ready ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE).json({ status: ready ? 'ok' : 'unavailable' });
  }
  @Get('metrics') async getMetrics(@Res() res: any): Promise<void> {
    try {
      const rows: { lag: string }[] = await this.ds.query(`SELECT COALESCE(EXTRACT(EPOCH FROM now()-MIN(occurred_at)),0)::text AS lag
        FROM outbox_messages WHERE published_at IS NULL`);
      gauge('wager_outbox_lag_seconds', Number(rows[0]?.lag ?? '0'));
    } catch { gauge('wager_outbox_lag_seconds', -1); }
    res.type('text/plain').send(metrics());
  }
}

@Module({ controllers: [WalletController, WagerController, ProviderTransactionController, HealthController], providers: [
  ProviderIdentityGuard,
  { provide: 'DS', useFactory: async () => makeDataSource().initialize() },
  { provide: 'SERVICE', useFactory: (ds: DataSource) => new WagerService(ds), inject: ['DS'] },
  { provide: 'WORKERS', useFactory: (ds: DataSource, service: WagerService) => new Workers(sqsClient(), ds, service), inject: ['DS', 'SERVICE'] },
] }) class AppModule {}

const app = await NestFactory.create(AppModule, { logger: false });
app.useGlobalFilters(new ApiErrors());
await app.listen(Number(process.env.PORT ?? 3000), '0.0.0.0');
console.log(JSON.stringify({ level: 'info', event: 'server_started', port: Number(process.env.PORT ?? 3000) }));
const workers = app.get<Workers>('WORKERS');
workers.start();
const shutdown = async () => { await workers.stop(); await app.close(); await app.get<DataSource>('DS').destroy(); process.exit(0); };
process.once('SIGTERM', () => { void shutdown(); });
process.once('SIGINT', () => { void shutdown(); });
