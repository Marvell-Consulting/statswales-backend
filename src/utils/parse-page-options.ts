import { Request } from 'express';

import { Locale } from '../enums/locale';
import { FieldValidationError, matchedData } from 'express-validator';
import { isDownloadFormat, OutputFormats } from '../enums/output-formats';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { PageOptions } from '../interfaces/page-options';
import { format2Validator, pageNumberValidator, pageSizeValidator } from '../validators';
import { logger } from './logger';
import { DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE } from './page-defaults';
import { parseSortByParam } from './parse-sort-by-param';

const defaultPageSize = (format: OutputFormats): number | undefined => {
  return isDownloadFormat(format) ? undefined : DEFAULT_PAGE_SIZE;
};

export async function parsePageOptions(req: Request): Promise<PageOptions> {
  logger.debug('Parsing page options from request...');
  const validations = [format2Validator(), pageNumberValidator(), pageSizeValidator()];

  for (const validation of validations) {
    const result = await validation.run(req);
    if (!result.isEmpty()) {
      const error = result.array()[0] as FieldValidationError;
      throw new BadRequestException(`${error.msg} for ${error.path}`);
    }
  }

  const params = matchedData(req);
  const format = (params.format as OutputFormats) ?? OutputFormats.Json;
  const pageNumber = params.page_number ?? 1;
  const pageSize = params.page_size ?? defaultPageSize(format);
  const locale = req.language as Locale;
  const sort = parseSortByParam(req.query.sort_by as string);

  if (!isDownloadFormat(format) && pageSize !== undefined && pageSize > MAX_PAGE_SIZE) {
    throw new BadRequestException(`page_size must not exceed ${MAX_PAGE_SIZE}`);
  }

  return { format, pageNumber, pageSize, sort, locale };
}
