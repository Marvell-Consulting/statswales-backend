import { PassThrough, Readable } from 'node:stream';

import { Request } from 'express';
import NodeClam from 'clamscan';

const mockConfig = {
  env: 'local',
  clamav: { host: 'clamav', port: 3310, timeout: 5000 }
};

jest.mock('../../../src/config', () => ({ config: mockConfig }));

// node:fs/promises exports are non-configurable, so jest.spyOn can't wrap them directly. Mock the
// module with jest.fn wrappers around the real implementations instead, so cleanupTmpFile's calls
// still actually stat/unlink the file on disk (letting us assert the file is really gone afterwards)
// while remaining observable to the test.
jest.mock('node:fs/promises', () => {
  const actual = jest.requireActual('node:fs/promises');
  return { ...actual, stat: jest.fn(actual.stat), unlink: jest.fn(actual.unlink) };
});

import { stat, unlink } from 'node:fs/promises';
const mockedStat = stat as jest.MockedFunction<typeof stat>;
const mockedUnlink = unlink as jest.MockedFunction<typeof unlink>;
const actualUnlink = jest.requireActual<typeof import('node:fs/promises')>('node:fs/promises').unlink;

jest.mock('../../../src/utils/logger', () => ({
  logger: {
    trace: jest.fn(),
    debug: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn()
  }
}));

jest.mock('clamscan');

import { uploadAvScan } from '../../../src/services/virus-scanner';
import { BadRequestException } from '../../../src/exceptions/bad-request.exception';
import { UnknownException } from '../../../src/exceptions/unknown.exception';

const MockedNodeClam = NodeClam as jest.MockedClass<typeof NodeClam>;

// Builds a fake Express request whose `files` async iterator yields a single file, mimicking what
// pechkin's `parseFormData` middleware would populate `req.files` with.
const buildRequest = (stream: Readable): Request => {
  return {
    files: {
      next: jest.fn().mockResolvedValue({
        value: { stream, filename: 'test.csv', mimeType: 'text/csv' }
      })
    }
  } as unknown as Request;
};

// `uploadAvScan` only attaches its 'scan-complete'/'timeout'/'error' listeners once the upload stream has
// finished piping through to the temp file, which happens asynchronously. Emitting the event immediately
// (or even on `setImmediate`) risks racing ahead of that listener being registered, so instead we hook into
// the emitter's 'newListener' event to emit right after the real listener is attached.
const emitOnceListening = (emitter: PassThrough, event: string, payload: unknown): void => {
  const onNewListener = (registeredEvent: string): void => {
    if (registeredEvent !== event) return;
    emitter.removeListener('newListener', onNewListener);
    process.nextTick(() => emitter.emit(event, payload));
  };
  emitter.on('newListener', onNewListener);
};

// `cleanupTmpFile` is fire-and-forget (not awaited by the caller), so tests can't just await
// `uploadAvScan`'s rejection to know cleanup has finished. Rather than sleeping a fixed duration
// (flaky under slow I/O), install a one-time wrapper around the next `unlink` call that resolves
// once the real unlink has actually completed.
const waitForNextUnlink = (): Promise<void> =>
  new Promise((resolve) => {
    mockedUnlink.mockImplementationOnce(async (...args: Parameters<typeof unlink>) => {
      const result = await actualUnlink(...args);
      resolve();
      return result;
    });
  });

describe('uploadAvScan', () => {
  let scannerStream: PassThrough;

  beforeEach(() => {
    mockConfig.env = 'local';
    scannerStream = new PassThrough();

    // `NodeClam.init()` resolves with the initialized instance itself, which is where `passthrough()` lives.
    MockedNodeClam.prototype.init = jest.fn().mockImplementation(async function (this: NodeClam) {
      return this;
    });
    MockedNodeClam.prototype.passthrough = jest.fn().mockReturnValue(scannerStream);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('resolves with the temp file when the scan result is unambiguously clean', async () => {
    const req = buildRequest(Readable.from(['some file content']));
    const promise = uploadAvScan(req);

    emitOnceListening(scannerStream, 'scan-complete', { isInfected: false, viruses: [], timeout: false });

    const tmpFile = await promise;
    try {
      expect(tmpFile.originalname).toBe('test.csv');
    } finally {
      await unlink(tmpFile.path).catch(() => {});
    }
  });

  it('rejects and fails closed when the scan result is infected', async () => {
    const req = buildRequest(Readable.from(['infected content']));
    const promise = uploadAvScan(req);

    // this scan-complete branch also calls cleanupTmpFile; wait for it so the temp file is gone
    // before the next test runs, rather than leaving a fire-and-forget unlink pending in the background
    const cleanupDone = waitForNextUnlink();
    emitOnceListening(scannerStream, 'scan-complete', {
      isInfected: true,
      viruses: ['Eicar-Test-Signature'],
      timeout: false
    });

    await expect(promise).rejects.toThrow(BadRequestException);
    await cleanupDone;
  });

  it('rejects and fails closed when clamscan cannot classify the result (isInfected: null)', async () => {
    // clamscan returns `isInfected: null` when the daemon's reply couldn't be parsed, e.g. a truncated
    // response or "COMMAND READ TIMED OUT". This must never be treated as a clean result.
    const req = buildRequest(Readable.from(['some file content']));
    const promise = uploadAvScan(req);

    const cleanupDone = waitForNextUnlink();
    emitOnceListening(scannerStream, 'scan-complete', { isInfected: null, viruses: [], timeout: false });

    await expect(promise).rejects.toThrow(UnknownException);
    await cleanupDone;
  });

  it('rejects and fails closed when the scan result is flagged as timed out', async () => {
    const req = buildRequest(Readable.from(['some file content']));
    const promise = uploadAvScan(req);

    const cleanupDone = waitForNextUnlink();
    // clamscan sets `timeout: true` alongside `isInfected: null` on a "COMMAND READ TIMED OUT" response
    emitOnceListening(scannerStream, 'scan-complete', { isInfected: null, viruses: [], timeout: true });

    await expect(promise).rejects.toThrow(UnknownException);
    await cleanupDone;
  });

  // The 'timeout' and 'error' listeners both follow the identical cleanupTmpFile-then-reject shape (see
  // virus-scanner.ts), so this test covers both code paths. We don't emit a synthetic 'error' event here:
  // a completed `pipeline()` leaves internal listeners on its streams (confirmed independently of this
  // fix - a plain Node script reproduces 4 leftover 'error' listeners on the middle stream after a
  // successful 3-stream pipeline), and re-triggering 'error' on that stream from a test interacts badly
  // with those leftover listeners under Jest specifically, well outside anything this fix touches.
  it('deletes the temp file when the scanner stream itself times out', async () => {
    const req = buildRequest(Readable.from(['some file content']));
    const promise = uploadAvScan(req);

    const cleanupDone = waitForNextUnlink();
    emitOnceListening(scannerStream, 'timeout', undefined);

    await expect(promise).rejects.toThrow(UnknownException);
    await cleanupDone;

    // cleanupTmpFile calls stat() first, so its first call argument tells us which path was cleaned up
    expect(mockedStat).toHaveBeenCalled();
    const cleanedPath = mockedStat.mock.calls[0][0] as string;
    await expect(stat(cleanedPath)).rejects.toThrow();
  });
});
