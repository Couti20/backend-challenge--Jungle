import { makeDataSource } from '../src/db';
import { WagerService } from '../src/service';

const [walletId, playerId, externalTransactionId] = process.argv.slice(2);
if (!walletId || !playerId || !externalTransactionId) process.exit(2);
const ds = await makeDataSource().initialize();
try {
  const result = await new WagerService(ds).process({ providerId: 'multiprocess', externalTransactionId,
    playerId, walletId, roundId: 'round-1', gameId: 'game-1', kind: 'BET', money: { amount: '80.00', currency: 'BRL' } },
  `key:${externalTransactionId}`);
  console.log(JSON.stringify(result));
} finally { await ds.destroy(); }
