export enum FileValidationErrorType {
  LookupMissingValues = 'lookup_missing_values',
  LookupNoJoinColumn = 'lookup_no_join_column',
  UnknownMimeType = 'unknown_mime_type',
  UnknownFileFormat = 'unknown_file_format',
  WrongDataTypeInReference = 'wrong_data_type_in_reference',
  MissingLanguages = 'missing_languages',
  BadDecimalColumn = 'invalid_decimals_present',
  InvalidUnicode = 'invalid_unicode',
  InvalidCsv = 'invalid_csv',
  InvalidJson = 'invalid_json',
  FactTable = 'fact_table',
  DataLake = 'datalake_upload_error',
  unknown = 'unknown'
}

export class FileValidationException extends Error {
  status = 400;
  errorTag: string;
  message: string;
  type: FileValidationErrorType;
  extension: never;

  constructor(message: string, type: FileValidationErrorType, status = 400) {
    super(message);
    this.type = type;
    this.errorTag = `errors.file_validation.${type}`;
    this.status = status;
  }
}
