import { Writable } from 'node:stream';
import { Response } from 'express';

export type StalledResponse = Response & {
  resumeClient: () => void;
  body: () => string;
  buffer: () => Buffer;
  bytes: () => number;
};

// A response whose client has stopped reading. It accepts writes until its small buffer is full, then nothing drains
// until resumeClient() is called. Use it to check that a download waits for the client rather than reading ahead.
export function createStalledResponse(highWaterMark = 1024): StalledResponse {
  const chunks: Buffer[] = [];
  const held: (() => void)[] = [];
  let bytes = 0;
  let stalled = true;

  const stream = new Writable({
    highWaterMark,
    write(chunk: Buffer | string, _encoding, callback): void {
      const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      chunks.push(buf);
      bytes += buf.length;
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
  res.buffer = (): Buffer => Buffer.concat(chunks);
  res.bytes = (): number => bytes;
  return res;
}

// Lets pending promise and I/O callbacks run until the download stops making progress, so it gets as far as it can
// before the test checks it. `progress` is something that grows as the download runs, such as a count of rows read.
// Waiting until it stops changing for `quietRounds` turns of the event loop, rather than for a fixed number of turns,
// means the test doesn't depend on how fast the machine is.
export async function settle(progress: () => number, quietRounds = 20, maxRounds = 5000): Promise<void> {
  const tick = (): Promise<void> => new Promise((resolve) => setImmediate(resolve));
  let last = progress();
  let quiet = 0;
  for (let i = 0; i < maxRounds && quiet < quietRounds; i++) {
    await tick();
    const now = progress();
    quiet = now === last ? quiet + 1 : 0;
    last = now;
  }
}
