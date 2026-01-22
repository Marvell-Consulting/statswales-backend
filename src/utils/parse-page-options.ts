import { Request } from 'express';

import { Locale } from '../enums/locale';
import { FieldValidationError, matchedData } from 'express-validator';
import { OutputFormats } from '../enums/output-formats';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { PageOptions } from '../interfaces/page-options';
import { SortByInterface } from '../interfaces/sort-by-interface';
import { format2Validator, pageNumberValidator, pageSizeValidator } from '../validators';
import { logger } from './logger';
import { DEFAULT_PAGE_SIZE } from './page-defaults';
import { sortObjToString } from './sort-obj-to-string';

const defaultPageSize = (format: OutputFormats): number | undefined => {
  return format === OutputFormats.Frontend ? DEFAULT_PAGE_SIZE : undefined;
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
  let sort: string[] = [];

  try {
    const sortBy = req.query.sort_by ? (JSON.parse(req.query.sort_by as string) as SortByInterface[]) : undefined;
    sort = sortBy ? sortObjToString(sortBy) : [];
  } catch (_err) {
    throw new BadRequestException('errors.invalid_sort_by');
  }

  return { format, pageNumber, pageSize, sort, locale };
}
