export enum FileValidationErrorType {
  UnknownMimeType = 'unknown_mime_type',
  UnknownFileFormat = 'unknown_file_format',
  InvalidUnicode = 'invalid_unicode',
  InvalidCsv = 'invalid_csv',
  InvalidJson = 'invalid_json',
  FactTable = 'fact_table',
  datalake = 'datalake_upload_error',
  unknown = 'unknown'
}

export class FileValidationException extends Error {
  status = 400;
  errorTag: string;
  message: string;
  type: FileValidationErrorType;

  constructor(message: string, type: FileValidationErrorType, status = 400) {
    super(message);
    this.type = type;
    this.errorTag = `errors.file_validation.${type}`;
    this.status = status;
  }
}
