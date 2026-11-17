import { logger } from './logger';

/*
 * The server starts listening before it has connected to the database, so the platform's startup and liveness
 * probes pass while it connects. Until startup is complete, /healthcheck/ready reports not ready, so the platform
 * sends no traffic here, and every other route returns 503 in case a request arrives anyway (startup-gate.ts).
 * If the database is unreachable, startup keeps trying instead of exiting: exiting makes the platform restart the
 * container with growing back-off delays, which keeps the service down for minutes after the database is back.
 */

// Complete unless the server marks it pending as it starts, so code that imports the app without the server, such
// as the tests, isn't affected.
let startupComplete = true;

export const isStartupComplete = (): boolean => startupComplete;

export const markStartupPending = (): void => {
  startupComplete = false;
};

export const markStartupComplete = (): void => {
  startupComplete = true;
};

// Network errors from Node, and Postgres SQLSTATEs for a server that is restarting or can't be reached
// (class 08 connection exceptions, 57P01-57P03 shutdown and startup).
const CONNECTION_ERROR_CODES = new Set([
  'ECONNREFUSED',
  'ECONNRESET',
  'ETIMEDOUT',
  'ENOTFOUND',
  'EAI_AGAIN',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'EPIPE',
  '57P01',
  '57P02',
  '57P03',
  '08000',
  '08001',
  '08003',
  '08004',
  '08006',
  '08007'
]);

// Errors that pg and PgBouncer raise without a code, or with a code that also covers other problems.
const CONNECTION_ERROR_MESSAGE =
  /timeout expired|timeout exceeded when trying to connect|Connection terminated|server login has been failing|the database system is (starting up|shutting down)|no more connections allowed/i;

interface ErrorLike {
  code?: unknown;
  message?: unknown;
  cause?: unknown;
  driverError?: unknown;
  errors?: unknown;
}

/** Whether an error means the database couldn't be reached, as opposed to a problem with a query or login. */
export const isConnectionError = (err: unknown, seen = new Set<unknown>()): boolean => {
  if (!err || typeof err !== 'object' || seen.has(err)) return false;
  seen.add(err);

  const { code, message, cause, driverError, errors } = err as ErrorLike;
  if (typeof code === 'string' && CONNECTION_ERROR_CODES.has(code)) return true;
  if (typeof message === 'string' && CONNECTION_ERROR_MESSAGE.test(message)) return true;
  if (Array.isArray(errors) && errors.some((e) => isConnectionError(e, seen))) return true;

  return isConnectionError(driverError, seen) || isConnectionError(cause, seen);
};

/**
 * Picks the error to report when several things failed at once. Any error that isn't a connection error comes first,
 * so a problem that retrying won't fix, such as a failed login, stops startup instead of being retried.
 */
export const pickStartupError = (errors: unknown[]): unknown =>
  errors.find((err) => !isConnectionError(err)) ?? errors[0];

export interface RetryOptions {
  initialDelayMs?: number;
  maxDelayMs?: number;
  sleep?: (ms: number) => Promise<void>;
}

const wait = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Runs the task until it succeeds. Connection errors are logged and retried, waiting twice as long each time up to
 * maxDelayMs. Any other error is thrown at once.
 */
export const retryWhileDatabaseUnreachable = async <T>(task: () => Promise<T>, opts: RetryOptions = {}): Promise<T> => {
  const { initialDelayMs = 1000, maxDelayMs = 30000, sleep = wait } = opts;
  let delay = initialDelayMs;

  for (let attempt = 1; ; attempt++) {
    try {
      return await task();
    } catch (err) {
      if (!isConnectionError(err)) throw err;
      logger.warn({ err, attempt, retryInMs: delay }, 'Could not reach the database during startup, will try again');
      await sleep(delay);
      delay = Math.min(delay * 2, maxDelayMs);
    }
  }
};
