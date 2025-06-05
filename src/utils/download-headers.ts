/* eslint-disable @typescript-eslint/naming-convention */

import { OutgoingHttpHeaders } from 'node:http2';

import { DownloadFormat } from '../enums/download-format';
import { BadRequestException } from '../exceptions/bad-request.exception';

export const getDownloadHeaders = (datasetId: string, format: string, contentLength: number): OutgoingHttpHeaders => {
  const defaultHeaders: OutgoingHttpHeaders = {
    'content-disposition': `attachment;filename=${datasetId}.${format}`,
    'content-length': contentLength
  };

  switch (format) {
    case DownloadFormat.Csv:
      return { ...defaultHeaders, 'content-type': 'text/csv; charset=utf-8' };

    case DownloadFormat.DuckDb:
      return { ...defaultHeaders, 'content-type': 'application/octet-stream' };

    case DownloadFormat.Json:
      return { ...defaultHeaders, 'content-type': 'application/json; charset=utf-8' };

    case DownloadFormat.Parquet:
      return { ...defaultHeaders, 'content-type': 'application/vnd.apache.parquet' };

    case DownloadFormat.Xlsx:
      return { ...defaultHeaders, 'content-type': 'application/vnd.ms-excel' };

    default:
      throw new BadRequestException('unsupported file format');
  }
};
