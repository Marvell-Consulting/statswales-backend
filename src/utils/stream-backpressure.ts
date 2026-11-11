import { Duplex, Writable } from 'node:stream';

export class StreamClosedError extends Error {
  constructor() {
    super('The stream closed before all the data was written');
    this.name = 'StreamClosedError';
  }
}

// True when an error from a download means the client went away before it finished, rather than something failing.
// Covers our own StreamClosedError and errors such as ERR_STREAM_PREMATURE_CLOSE from pipeline() when the response
// is destroyed.
export function isClientDisconnect(err: unknown, res: Writable): boolean {
  return err instanceof StreamClosedError || (res.destroyed && !res.writableFinished);
}

function isClosed(stream: Writable): boolean {
  return stream.destroyed || stream.writableEnded;
}

// Rejects as soon as any of `streams` closes before finishing, or errors. A stream that closes after it has finished
// writing (for example a response that exceljs ended itself) closed normally, so that doesn't count. Returns a
// function that stops listening.
function watchForClose(streams: Writable[], reject: (err: Error) => void): () => void {
  const listeners = streams.map((s) => {
    const onClose = (): void => {
      if (!s.writableFinished) reject(new StreamClosedError());
    };
    const onError = (err: Error): void => reject(err);
    s.on('close', onClose);
    s.on('error', onError);
    return { s, onClose, onError };
  });
  return () => {
    listeners.forEach(({ s, onClose, onError }) => {
      s.off('close', onClose);
      s.off('error', onError);
    });
  };
}

// Resolves once `stream` can take more data. Downloads call this so they stop reading from the database while the
// client is behind, rather than holding the rest of the result in memory.
//
// Rejects if the stream closes or errors first, for example when the client disconnects, so the caller can stop and
// release its database connection. When `stream` is piped into a response, pass the response as `destination` too:
// if the client disconnects, the piped stream never drains, so we have to watch the response as well.
export async function waitForDrain(stream: Writable, destination?: Writable): Promise<void> {
  const streams = destination ? [stream, destination] : [stream];
  if (streams.some(isClosed)) {
    throw new StreamClosedError();
  }
  if (!stream.writableNeedDrain) {
    return;
  }

  await new Promise<void>((resolve, reject) => {
    const stopWatching = watchForClose(streams, (err) => {
      stream.off('drain', onDrain);
      stopWatching();
      reject(err);
    });
    const onDrain = (): void => {
      stopWatching();
      resolve();
    };
    stream.once('drain', onDrain);
  });
}

// For Excel downloads. exceljs compresses the file in the background, so its output only reaches the response after
// the event loop has had a turn. Give it one before checking whether the response is full, otherwise a fast database
// read can run far ahead of what has actually been sent.
export async function waitForCompressedDrain(stream: Writable): Promise<void> {
  await new Promise((resolve) => setImmediate(resolve));
  await waitForDrain(stream);
}

// Writes `chunk` to `stream`, then waits if the stream's buffer is full. See waitForDrain for `destination`.
export async function writeAndWait(stream: Writable, chunk: unknown, destination?: Writable): Promise<void> {
  if (isClosed(stream) || (destination && isClosed(destination))) {
    throw new StreamClosedError();
  }
  if (!stream.write(chunk)) {
    await waitForDrain(stream, destination);
  }
}

// Waits for `work`, but rejects straight away if `stream` closes or errors first. Use it for steps that would
// otherwise wait forever on a client that has gone, such as finishing an Excel file.
export async function unlessClosed<T>(stream: Writable, work: Promise<T>): Promise<T> {
  if (stream.destroyed) {
    throw new StreamClosedError();
  }
  // If the stream closes first, `work` may still reject later. Nobody is waiting for it then, so ignore that.
  work.catch(() => {});
  let stopWatching = (): void => {};
  const closed = new Promise<never>((_resolve, reject) => {
    stopWatching = watchForClose([stream], reject);
  });
  try {
    return await Promise.race([work, closed]);
  } finally {
    stopWatching();
    if (stream.destroyed && !stream.writableFinished) {
      // `work` may still be running and write to the closed stream, which can emit 'error' later. Nobody is
      // listening by then, and an unhandled 'error' event would crash the process, so ignore it.
      stream.on('error', () => {});
    }
  }
}

// Ends `source`, which is piped into `destination`, and waits until `destination` has finished. Use it instead of
// ending the destination directly, which can cut off data still buffered in the source.
//
// Rejects with StreamClosedError if the destination closes first (the client disconnected). Rejects with the source's
// error if the source fails while flushing: it then stops feeding the destination, which would otherwise never
// finish. This listens for events directly because `finished()` treats a source that has ended its writable side as
// done, even if it then fails before passing everything on.
//
// Pass `endsDestination: false` when the source was piped with `{ end: false }`. It then resolves once the source has
// passed everything on, and the caller ends the destination itself.
export async function endPipedStream(
  source: Duplex,
  destination: Writable,
  { endsDestination = true }: { endsDestination?: boolean } = {}
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const doneEvent = endsDestination ? 'finish' : 'end';
    const doneEmitter = endsDestination ? destination : source;
    const cleanup = (): void => {
      doneEmitter.off(doneEvent, onFinish);
      destination.off('close', onDestinationClose);
      destination.off('error', onError);
      source.off('error', onError);
      source.off('close', onSourceClose);
    };
    const onFinish = (): void => {
      cleanup();
      resolve();
    };
    const onDestinationClose = (): void => {
      if (destination.writableFinished) return;
      cleanup();
      reject(new StreamClosedError());
    };
    const onError = (err: Error): void => {
      cleanup();
      reject(destination.destroyed && !destination.writableFinished ? new StreamClosedError() : err);
    };
    const onSourceClose = (): void => {
      if (source.readableEnded) return;
      cleanup();
      reject(source.errored ?? new Error('The source stream closed before passing on all its data'));
    };

    doneEmitter.on(doneEvent, onFinish);
    destination.on('close', onDestinationClose);
    destination.on('error', onError);
    source.on('error', onError);
    source.on('close', onSourceClose);
    source.end();
  });
}
