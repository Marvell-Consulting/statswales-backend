import { Pool } from 'pg';
import { appConfig } from '../config';

const config = appConfig().database;

let cubeDB: Pool | undefined = undefined;

/**
 * Returns a singleton of the CubeDB Pool.
 *
 * This is the cube database configuration. For the main application database, see data-source.ts.
 */
export function getCubeDB(): Pool {
  if (!cubeDB) {
    cubeDB = new Pool({
      host: config.host,
      port: config.port,
      ssl: config.ssl,
      user: config.username,
      password: config.password,
      database: config.database,
      max: 10,
      idleTimeoutMillis: 1000,
      connectionTimeoutMillis: 1000,
      maxUses: 7500
    });
  }
  return cubeDB;
}
