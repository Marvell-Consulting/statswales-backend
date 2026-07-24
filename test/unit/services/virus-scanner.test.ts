import { PassThrough, Readable } from 'node:stream';

import { Request } from 'express';
import NodeClam from 'clamscan';

const mockConfig = {
  env: 'local',
  clamav: { host: 'clamav', port: 3310, timeout: 5000 }
};

jest.mock('../../../src/config', () => ({ config: mockConfig }));

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

// `uploadAvScan` only attaches its 'scan-complete' listener once the upload stream has finished
// piping through to the temp file, which happens asynchronously. Emitting the event immediately
// (or even on `setImmediate`) risks racing ahead of that listener being registered, so instead we
// hook into the emitter's 'newListener' event to emit right after the real listener is attached.
const emitOnceListening = (emitter: PassThrough, event: string, payload: unknown): void => {
  const onNewListener = (registeredEvent: string): void => {
    if (registeredEvent !== event) return;
    emitter.removeListener('newListener', onNewListener);
    process.nextTick(() => emitter.emit(event, payload));
  };
  emitter.on('newListener', onNewListener);
};

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
      const { unlink } = await import('node:fs/promises');
      await unlink(tmpFile.path).catch(() => {});
    }

  it('rejects and fails closed when the scan result is infected', async () => {
    const req = buildRequest(Readable.from(['infected content']));
    const promise = uploadAvScan(req);

    emitOnceListening(scannerStream, 'scan-complete', {
      isInfected: true,
      viruses: ['Eicar-Test-Signature'],
      timeout: false
    });

    await expect(promise).rejects.toThrow(BadRequestException);
  });

  it('rejects and fails closed when clamscan cannot classify the result (isInfected: null)', async () => {
    // clamscan returns `isInfected: null` when the daemon's reply couldn't be parsed, e.g. a truncated
    // response or "COMMAND READ TIMED OUT". This must never be treated as a clean result.
    const req = buildRequest(Readable.from(['some file content']));
    const promise = uploadAvScan(req);

    emitOnceListening(scannerStream, 'scan-complete', { isInfected: null, viruses: [], timeout: false });

    await expect(promise).rejects.toThrow(UnknownException);
  });

  it('rejects and fails closed when the scan result is flagged as timed out', async () => {
    const req = buildRequest(Readable.from(['some file content']));
    const promise = uploadAvScan(req);

    // clamscan sets `timeout: true` alongside `isInfected: null` on a "COMMAND READ TIMED OUT" response
    emitOnceListening(scannerStream, 'scan-complete', { isInfected: null, viruses: [], timeout: true });

    await expect(promise).rejects.toThrow(UnknownException);
  });
});
