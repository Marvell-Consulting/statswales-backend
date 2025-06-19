import { Request, Response, NextFunction } from 'express';
import { parseFormData } from 'pechkin';

import { Internal } from 'pechkin/dist/types.js';

// Pechkin is a wrapper around busboy that makes it awaitable and provides a more convenient API for handling
// file uploads in Express.js applications.

// This middleware returns a function that processes multipart/form-data requests, extracting file streams.
export const fileStreaming = (
  config?: Partial<Internal.Config>,
  fileFieldConfigOverride?: Internal.FileFieldConfigOverride,
  busboyConfig?: Internal.BusboyConfig
) => {
  return async (req: Request, res: Response, next: NextFunction) => {
    try {
      const { fields, files } = await parseFormData(req, config, fileFieldConfigOverride, busboyConfig);
      req.body = fields;
      req.files = files;
      return next();
    } catch (err) {
      return next(err);
    }
  };
};
