export enum FactTableValidationExceptionType {
  UnknownSourcesStillPresent = 'unknown_present',
  FactTableCreationFailed = 'fact_table_creation_failed',
  NoNoteCodes = 'no_note_codes',
  BadNoteCodes = 'bad_note_codes',
  NoDraftRevision = 'no_draft_revision',
  NoDataTable = 'no_data_table',
  FailedToLoadData = 'failed_to_load_data',
  DuplicateFact = 'duplicate_fact',
  IncompleteFact = 'incomplete_fact',
  NoDataValueColumn = 'no_data_value_column',
  NonNumericDataValueColumn = 'non_numeric_data_value',
  UnknownError = 'unknown_error',
  UnknownFileType = 'unknown_file_type',
  UnmatchedColumns = 'unmatched_columns'
}
