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

  const av = clamscan.passthrough();

  av.on('error', (err) => {
    throw err;
  });

  av.on('timeout', () => {
    throw new Error('Virus scan timed out');
  });

  return av;
};

type SuccessCallback = (tmpFile: TempFile) => void;

// Creates a temporary file stream in the system's temp directory.
// When the file finishes writing, it calls the provided callback with the file path.
const getTmpFileStream = (tmpFile: TempFile, onFinish: SuccessCallback): WriteStream => {
  const tmp = os.tmpdir();
  const randomName = crypto.randomBytes(16).toString('hex');
  const filePath = path.resolve(tmp, randomName);
  const outputFile = createWriteStream(filePath, { flags: 'w' });

  outputFile.on('error', (err: any) => {
    throw err;
  });

  outputFile.on('finish', () => {
    logger.debug(`File uploaded temporarily to ${filePath}`);
    onFinish({ ...tmpFile, path: filePath, size: outputFile.bytesWritten });
  });

  return outputFile;
};

// Handles file upload and virus scanning. Expects a single file in the request, streams it to the ClamAV scanner,
// and writes the file to a temporary location. If the file is infected, it throws an error and deletes the file.
// Note: This function assumes that the request has been processed by a middleware that populates req.files with the
// uploaded files streams. If we want to handle multiple files in a single request, we would need to iterate over
// req.files, see https://github.com/rafasofizada/pechkin/blob/master/examples/express.js
export const uploadAvScan = async (req: Request): Promise<TempFile> => {
  const start = performance.now();
  const iterable = ((await req.files?.next()) as any) || {};
  const { stream, filename, mimeType } = iterable.value || {};

  if (!stream || !filename || !mimeType) {
    throw new BadRequestException('errors.upload.file_missing');
  }

  let virusScanner: Transform;
  let tmpFileStream: WriteStream;
  let tmpFile: TempFile = { path: '', originalname: filename, mimetype: mimeType };

  try {
    virusScanner = await getAVPassthrough();
    tmpFileStream = getTmpFileStream(tmpFile, (finished: TempFile) => (tmpFile = finished));
  } catch (err) {
    logger.error(err, 'There was a problem initializing the virus scanner or temporary file stream');
    throw new UnknownException('errors.upload.initialization_failure');
  }

  await pipeline(stream, virusScanner, tmpFileStream).catch((err: any) => {
    logger.error(err, 'There was a problem streaming the file upload');
    throw new UnknownException('errors.upload.stream_failure');
  });

  // wait for the scan to complete before returning the temporary file
  return new Promise((resolve, reject) => {
    virusScanner.on('scan-complete', async (result) => {
      if (result.isInfected) {
        logger.error(`File ${filename} is infected with virus: ${result.viruses.join(', ')}`);

        // delete the infected temp file
        await stat(tmpFile.path)
          .then(() => unlink(tmpFile.path))
          .catch(() => {});

        reject(new BadRequestException('errors.upload.infected'));
        return;
      }

      const time = Math.round(performance.now() - start);
      logger.info(`AV Scan complete. File "${filename}" is clean, took ${time}ms`);
      resolve(tmpFile);
    });
  });
};
