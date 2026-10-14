import { makeDataSource } from './db';
import { up, down } from './migration';

const ds = await makeDataSource().initialize();
try {
  await ds.transaction(async manager => {
    await manager.query('SELECT pg_advisory_xact_lock(498244539)');
    await manager.query('CREATE TABLE IF NOT EXISTS schema_migrations (version integer PRIMARY KEY)');
    const rows: { version: number }[] = await manager.query('SELECT version FROM schema_migrations WHERE version = 1');
    if (process.argv[2] === 'down') {
      if (rows.length) { await manager.query(down); await manager.query('DELETE FROM schema_migrations WHERE version = 1'); }
    } else if (!rows.length) {
      await manager.query(up);
      await manager.query('INSERT INTO schema_migrations(version) VALUES (1)');
    }
  });
} finally { await ds.destroy(); }
