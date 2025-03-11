import { Request, Response, NextFunction } from 'express';
import { v4 as uuid } from 'uuid';

import { asyncLocalStorage } from '../services/async-local-storage';

// Creates a context for storing values for the duration of the request
// Any code running after this middlware can then access the "store" Map using
// const context = asyncLocalStorage.getStore();
// context.set('x', 'foo')
// const x = context.get('x')
// @see https://medium.com/wix-engineering/solving-the-async-context-challenge-in-node-js-088864aa715e
// for a more complete example
export const requestContext = async (req: Request, res: Response, next: NextFunction) => {
  const store = new Map<string, any>();
  store.set('requestId', uuid());
  asyncLocalStorage.run(store, next);
};
