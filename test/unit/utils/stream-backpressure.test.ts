import { Writable } from 'node:stream';

import { StreamClosedError, unlessClosed, waitForDrain, writeAndWait } from '../../../src/utils/stream-backpressure';

// A writable that holds every write until release() is called, like a client that has stopped reading.
function createHeldWritable(highWaterMark = 16): Writable & { release: () => void } {
  const held: (() => void)[] = [];
  let holding = true;
  const stream = new Writable({
    highWaterMark,
    write(_chunk, _encoding, callback): void {
      if (holding) held.push(() => callback());
      else callback();
    }
  }) as Writable & { release: () => void };
  stream.release = (): void => {
    holding = false;
    held.splice(0).forEach((release) => release());
  };
  return stream;
}

describe('stream-backpressure', () => {
  describe('writeAndWait', () => {
    it('returns straight away while the stream has room', async () => {
      const stream = createHeldWritable(1024);
      await expect(writeAndWait(stream, 'small')).resolves.toBeUndefined();
    });

    it('waits until the stream drains when its buffer is full', async () => {
      const stream = createHeldWritable();
      let resolved = false;

      const write = writeAndWait(stream, 'x'.repeat(100)).then(() => (resolved = true));
      await new Promise((resolve) => setImmediate(resolve));
      expect(resolved).toBe(false);

      stream.release();
      await write;
      expect(resolved).toBe(true);
    });

    it('rejects if the stream closes while waiting', async () => {
      const stream = createHeldWritable();
      const write = writeAndWait(stream, 'x'.repeat(100));
      stream.destroy();
      await expect(write).rejects.toBeInstanceOf(StreamClosedError);
    });

    it('rejects if the destination closes while waiting on a piped stream', async () => {
      const source = createHeldWritable();
      const destination = createHeldWritable();
      const write = writeAndWait(source, 'x'.repeat(100), destination);
      destination.destroy();
      await expect(write).rejects.toBeInstanceOf(StreamClosedError);
    });

    it('rejects straight away if the stream has already closed', async () => {
      const stream = createHeldWritable();
      stream.destroy();
      await expect(writeAndWait(stream, 'x')).rejects.toBeInstanceOf(StreamClosedError);
    });
  });

  describe('waitForDrain', () => {
    it('returns straight away when the stream does not need to drain', async () => {
      await expect(waitForDrain(createHeldWritable())).resolves.toBeUndefined();
    });
  });

  describe('unlessClosed', () => {
    it('returns the result of the work', async () => {
      await expect(unlessClosed(createHeldWritable(), Promise.resolve('done'))).resolves.toBe('done');
    });

    it('rejects if the stream closes before the work finishes', async () => {
      const stream = createHeldWritable();
      const result = unlessClosed(stream, new Promise(() => {}));
      stream.destroy();
      await expect(result).rejects.toBeInstanceOf(StreamClosedError);
    });

    it('does not treat a stream closing after it has finished as a disconnect', async () => {
      const stream = createHeldWritable(1024);
      stream.release();
      const work = new Promise<string>((resolve) => {
        stream.end('last', () => setImmediate(() => resolve('done')));
      });
      await expect(unlessClosed(stream, work)).resolves.toBe('done');
    });
  });
});
