import { logger } from './logger';

/*
 * The server starts listening before it has connected to the database, so the platform's startup and liveness
 * probes pass while it connects. /healthcheck/ready reports not ready until startup is complete, so no traffic is
 * sent to this instance before then. If the database is unreachable, startup keeps trying instead of exiting:
 * exiting makes the platform restart the container with growing back-off delays, which keeps the service down
 * for minutes after the database is back.
 */

let startupComplete = false;

export const isStartupComplete = (): boolean => startupComplete;

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
  '08006'
]);

// Errors that pg and PgBouncer raise without a code, or with a code that also covers other problems.
const CONNECTION_ERROR_MESSAGE =
  /timeout expired|timeout exceeded when trying to connect|Connection terminated|server login has been failing|the database system is (starting up|shutting down)/i;

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
