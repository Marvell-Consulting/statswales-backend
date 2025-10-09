import { randomUUID } from 'node:crypto';

export const uuidV4 = (): string => {
  return randomUUID().toLowerCase();
};
