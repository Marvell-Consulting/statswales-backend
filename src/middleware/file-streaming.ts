import { Request, Response, NextFunction } from 'express';
import { parseFormData } from 'pechkin';
import { Internal } from 'pechkin/dist/types.js';
import { merge } from 'lodash';

import { logger } from '../utils/logger';
import { BadRequestException } from '../exceptions/bad-request.exception';

// Pechkin is a wrapper around busboy that makes it awaitable and provides a more convenient API for handling
// file uploads in Express.js applications.

// This middleware returns a function that processes multipart/form-data requests, extracting file streams.
export const fileStreaming = (
  config?: Partial<Internal.Config>,
  fileFieldConfigOverride?: Internal.FileFieldConfigOverride,
  busboyConfig?: Internal.BusboyConfig
) => {
  return async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    const defaultFileConfig: Partial<Internal.Config> = {
      maxFileByteLength: 500 * 1024 * 1024 // 500MB
    };

    const finalConfig = merge({}, defaultFileConfig, config);

    try {
      const { fields, files } = await parseFormData(req, finalConfig, fileFieldConfigOverride, busboyConfig);
      req.body = fields;
      req.files = files;
      return next();
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } catch (err: any) {
      logger.warn(err, 'error attempting to parse multipart/form-data');
      next(new BadRequestException('errors.upload.failed_to_parse'));
      return;
    }
  };
};
