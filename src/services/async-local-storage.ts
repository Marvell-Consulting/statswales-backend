import { AsyncLocalStorage } from 'node:async_hooks';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const asyncLocalStorage = new AsyncLocalStorage<Map<string, any>>();
