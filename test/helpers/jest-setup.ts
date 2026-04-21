// environment vars for jest
process.env.FRONTEND_URL = 'http://example.com:3000';
process.env.BACKEND_URL = 'http://example.com:3001';
process.env.SESSION_SECRET = 'mysecret';
process.env.JWT_SECRET = 'mysecret';
process.env.AUTH_PROVIDERS = 'local';

process.env.TEST_DB_HOST = 'localhost';
process.env.TEST_DB_PORT = '5433';
process.env.TEST_DB_USERNAME = 'postgres';
process.env.TEST_DB_PASSWORD = 'postgres';
// TEST_DB_DATABASE is set per-worker in worker-db-env.ts (integration) or falls back to the
// default in ci.ts ('statswales-backend-test') for unit tests that don't touch the DB.

process.env.APP_ENV = 'ci';
