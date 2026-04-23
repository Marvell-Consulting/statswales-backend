import { Client } from 'pg';

import { config } from '../config';

// Use a one-off connection rather than the pool so a saturated pool can't make
// the readiness probe hang past Azure's probe timeout.
//
// connectionTimeoutMillis bounds the TCP/TLS handshake; query_timeout is a
// client-side timer that fires a CancelRequest if SELECT 1 stalls. We avoid
// statement_timeout because pgbouncer in transaction pooling mode rejects
// unknown startup parameters and can't reliably preserve session-level SETs.
export const checkDb = async (): Promise<boolean> => {
  const timeoutMs = config.healthcheck.dbTimeoutMs;
  const client = new Client({
    host: config.database.host,
    port: config.database.port,
    user: config.database.username,
    password: config.database.password,
    database: config.database.database,
    ssl: config.database.ssl,
    application_name: 'sw3-backend-healthcheck',
    connectionTimeoutMillis: timeoutMs,
    query_timeout: timeoutMs
  });

  try {
    await client.connect();
    await client.query('SELECT 1 AS connected');
  } finally {
    await client.end().catch(() => undefined);
  }
  return true;
};
