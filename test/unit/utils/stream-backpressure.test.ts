import { PassThrough, Writable } from 'node:stream';

import {
  StreamClosedError,
  endPipedStream,
  unlessClosed,
  waitForDrain,
  writeAndWait
} from '../../../src/utils/stream-backpressure';

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

    it('does not leave the work unhandled when the stream has already closed', async () => {
      const unhandled: unknown[] = [];
      const onUnhandled = (reason: unknown): void => {
        unhandled.push(reason);
      };
      process.on('unhandledRejection', onUnhandled);
      try {
        const stream = createHeldWritable();
        stream.destroy();
        const work = new Promise((_resolve, reject) => setImmediate(() => reject(new Error('work failed later'))));

        await expect(unlessClosed(stream, work)).rejects.toBeInstanceOf(StreamClosedError);
        await new Promise((resolve) => setTimeout(resolve, 20));

        expect(unhandled).toEqual([]);
      } finally {
        process.off('unhandledRejection', onUnhandled);
      }
    });

    it('rejects if the stream closes before the work finishes', async () => {
      const stream = createHeldWritable();
      const result = unlessClosed(stream, new Promise(() => {}));
      stream.destroy();
      await expect(result).rejects.toBeInstanceOf(StreamClosedError);
    });

    it('leaves a stream that closed early safe from a late error event', async () => {
      const stream = createHeldWritable();
      const result = unlessClosed(stream, new Promise(() => {}));
      stream.destroy();
      await expect(result).rejects.toBeInstanceOf(StreamClosedError);

      // work that is still running, such as exceljs finishing a file, may make the stream emit an error later
      expect(() => stream.emit('error', new Error('late write'))).not.toThrow();
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

  describe('endPipedStream', () => {
    it('resolves once the destination has received everything', async () => {
      const source = new PassThrough();
      const destination = createHeldWritable(1024);
      destination.release();
      source.pipe(destination);
      source.write('last');

      await expect(endPipedStream(source, destination)).resolves.toBeUndefined();
      expect(destination.writableFinished).toBe(true);
    });

    it('rejects with StreamClosedError if the destination closes before it finishes', async () => {
      const source = new PassThrough();
      const destination = createHeldWritable();
      source.pipe(destination);
      source.write('x'.repeat(100));

      const ending = endPipedStream(source, destination);
      destination.destroy();
      await expect(ending).rejects.toBeInstanceOf(StreamClosedError);
    });

    it('with endsDestination: false, resolves once the source has passed everything on', async () => {
      const source = new PassThrough();
      const destination = createHeldWritable(1024);
      destination.release();
      source.pipe(destination, { end: false });
      source.write('last');

      await expect(endPipedStream(source, destination, { endsDestination: false })).resolves.toBeUndefined();
      expect(destination.writableEnded).toBe(false);
    });

    it('rejects, rather than waiting forever, if the source fails while flushing', async () => {
      const source = new PassThrough();
      const destination = createHeldWritable();
      source.pipe(destination);
      source.write('x'.repeat(100));

      const ending = endPipedStream(source, destination);
      source.destroy(new Error('formatting failed'));
      await expect(ending).rejects.toThrow('formatting failed');
    });
  });
});
