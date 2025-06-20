import crypto from 'node:crypto';
import { Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { createWriteStream, WriteStream } from 'node:fs';
import { stat, unlink } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';

import { Request } from 'express';
import NodeClam from 'clamscan';
import { Internal } from 'pechkin/dist/types.js';

import { appConfig } from '../config';
import { logger } from '../utils/logger';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { UnknownException } from '../exceptions/unknown.exception';
import { TempFile } from '../interfaces/temp-file';

// This stream transform will not wait for the scan to be performed before passing the stream through.
const getAVPassthrough = async (): Promise<Transform> => {
  const config = appConfig();
  const { host, port, timeout } = config.clamav;

  const clamscan = await new NodeClam().init({
    clamdscan: { host, port, timeout, localFallback: false }
  });

  logger.debug(`ClamAV scanner initialized with host: ${host}, port: ${port}, timeout: ${timeout}`);

  return clamscan.passthrough();
};

type SuccessCallback = (tmpFile: TempFile) => void;

// Creates a temporary file stream in the system's temp directory.
// When the file finishes writing, it calls the provided callback with the file path.
const getTmpFileStream = (tmpFile: TempFile, onFinish: SuccessCallback): WriteStream => {
  const tmp = os.tmpdir();
  const randomName = crypto.randomBytes(16).toString('hex');
  const filePath = path.resolve(tmp, randomName);
  const outputFile = createWriteStream(filePath, { flags: 'w' });

  outputFile.on('error', (err) => {
    throw err;
  });

  outputFile.on('finish', () => {
    logger.debug(`File uploaded temporarily to ${filePath}`);
    onFinish({ ...tmpFile, path: filePath, size: outputFile.bytesWritten });
  });

  return outputFile;
};

type FileIterator = { value?: Internal.File };

// Handles file upload and virus scanning. Expects a single file in the request, streams it to the ClamAV scanner,
// and writes the file to a temporary location. If the file is infected, it throws an error and deletes the file.
// Note: This function assumes that the request has been processed by a middleware that populates req.files with the
// uploaded files streams. If we want to handle multiple files in a single request, we would need to iterate over
// req.files, see https://github.com/rafasofizada/pechkin/blob/master/examples/express.js
export const uploadAvScan = async (req: Request): Promise<TempFile> => {
  const start = performance.now();
  const iterable = ((await req.files?.next()) as FileIterator) || {};
  const { stream, filename, mimeType } = iterable.value || {};

  if (!stream || !filename || !mimeType) {
    throw new BadRequestException('errors.file_upload.missing');
  }

  let virusScanner: Transform;
  let tmpFileStream: WriteStream;
  let tmpFile: TempFile = { path: '', originalname: filename, mimetype: mimeType };

  try {
    virusScanner = await getAVPassthrough();
    tmpFileStream = getTmpFileStream(tmpFile, (finished: TempFile) => (tmpFile = finished));
  } catch (err) {
    logger.error(err, 'There was a problem initializing the virus scanner or temporary file stream');
    throw new UnknownException('errors.file_upload.stream_failure');
  }

  // pipeline will not error on infected files, need to wait for 'scan-complete' event to know the result
  // it will wait for the file to finish writing before resolving the promise though
  await pipeline(stream, virusScanner, tmpFileStream).catch((err: unknown) => {
    logger.error(err, 'There was a problem streaming the file upload');
    throw new UnknownException('errors.file_upload.stream_failure');
  });

  // wait for the scan to complete before returning the temporary file
  return new Promise((resolve, reject) => {
    virusScanner.on('timeout', () => reject(new UnknownException('errors.file_upload.av_timeout')));

    virusScanner.on('error', (err) => {
      logger.error(err, 'There was a problem with the virus scanner');
      reject(new UnknownException('errors.file_upload.scan_failure'));
    });

    virusScanner.on('scan-complete', async (result) => {
      const time = Math.round(performance.now() - start);

      if (result.isInfected) {
        cleanupTmpFile(tmpFile);
        const viruses = result.viruses.join(', ');
        logger.warn(`AV Scan complete. File "${filename}" is infected with: "${viruses}", time: ${time}ms`);
        reject(new BadRequestException('errors.file_upload.infected'));
        return;
      }

      logger.info(`AV Scan complete. File "${filename}" is clean, time: ${time}ms`);
      resolve(tmpFile);
    });
  });
};

// treat this as fire and forget, don't wait for it to complete
export const cleanupTmpFile = async (tmpFile: TempFile): Promise<void> => {
  stat(tmpFile.path)
    .then(() => unlink(tmpFile.path))
    .catch(() => {}); // ignore errors
};
