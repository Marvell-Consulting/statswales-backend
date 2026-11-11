import { Writable } from 'node:stream';
import { Response } from 'express';

export type StalledResponse = Response & {
  resumeClient: () => void;
  body: () => string;
};

// A response whose client has stopped reading. It accepts writes until its small buffer is full, then nothing drains
// until resumeClient() is called. Use it to check that a download waits for the client rather than reading ahead.
export function createStalledResponse(highWaterMark = 1024): StalledResponse {
  const chunks: Buffer[] = [];
  const held: (() => void)[] = [];
  let stalled = true;

  const stream = new Writable({
    highWaterMark,
    write(chunk: Buffer | string, _encoding, callback): void {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      if (stalled) {
        held.push(() => callback());
      } else {
        callback();
      }
    }
  });

  const res = stream as unknown as StalledResponse;
  res.setHeader = jest.fn().mockReturnThis();
  res.writeHead = jest.fn().mockReturnThis();
  res.flushHeaders = jest.fn();
  res.status = jest.fn().mockReturnThis();
  res.json = jest.fn();
  Object.defineProperty(res, 'headersSent', { value: true, writable: true });
  res.resumeClient = (): void => {
    stalled = false;
    held.splice(0).forEach((release) => release());
  };
  res.body = (): string => Buffer.concat(chunks).toString();
  return res;
}

// Lets pending promise and I/O callbacks run, so a download gets as far as it can before the test checks it.
export async function settle(rounds = 50): Promise<void> {
  for (let i = 0; i < rounds; i++) {
    await new Promise((resolve) => setImmediate(resolve));
  }
}
