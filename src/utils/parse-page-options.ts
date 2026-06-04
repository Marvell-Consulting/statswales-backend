import { Request } from 'express';

import { Locale } from '../enums/locale';
import { FieldValidationError, matchedData } from 'express-validator';
import { isDownloadFormat, OutputFormats } from '../enums/output-formats';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { PageOptions } from '../interfaces/page-options';
import { cursorValidator, format2Validator, pageNumberValidator, pageSizeValidator } from '../validators';
import { logger } from './logger';
import { DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE } from './page-defaults';
import { parseSortByParam } from './parse-sort-by-param';

const defaultPageSize = (format: OutputFormats): number | undefined => {
  return isDownloadFormat(format) ? undefined : DEFAULT_PAGE_SIZE;
};

export interface ParsePageOptionsOpts {
  requireFormat?: boolean;
}

export async function parsePageOptions(req: Request, opts: ParsePageOptionsOpts = {}): Promise<PageOptions> {
  logger.debug('Parsing page options from request...');
  const validations = [format2Validator(), pageNumberValidator(), pageSizeValidator(), cursorValidator()];

  for (const validation of validations) {
    const result = await validation.run(req);
    if (!result.isEmpty()) {
      const error = result.array()[0] as FieldValidationError;
      throw new BadRequestException(`${error.msg} for ${error.path}`);
    }
  }

  const params = matchedData(req);
  const formatParam = params.format as OutputFormats | undefined;

  if (opts.requireFormat && !formatParam) {
    throw new BadRequestException('errors.output_format_required');
  }

  const format = formatParam ?? OutputFormats.Json;
  const pageNumber = params.page_number ?? 1;
  const pageSize = params.page_size ?? defaultPageSize(format);
  const locale = req.language as Locale;
  const sort = parseSortByParam(req.query.sort_by as string);
  const cursor = typeof params.cursor === 'string' ? params.cursor : undefined;

  if (!isDownloadFormat(format) && pageSize !== undefined && pageSize > MAX_PAGE_SIZE) {
    throw new BadRequestException(`page_size must not exceed ${MAX_PAGE_SIZE}`);
  }

  // page_number and cursor are mutually exclusive — the caller has to pick a
  // single pagination mode per request. page_number defaults to 1 when
  // omitted, so a cursor + explicit page_number > 1 is what we reject.
  if (cursor && (params.page_number ?? 1) > 1) {
    throw new BadRequestException('errors.cursor_and_page_number_mutually_exclusive');
  }

  return { format, pageNumber, pageSize, sort, locale, cursor };
}
