export enum FactTableValidationExceptionType {
  UnknownSourcesStillPresent = 'unknown_present',
  FactTableCreationFailed = 'fact_table_creation_failed',
  NoDraftRevision = 'no_draft_revision',
  NoDataTable = 'no_data_table',
  FailedToLoadData = 'failed_to_load_data',
  EmptyValue = 'incomplete_fact',
  DuplicateFact = 'duplicate_fact',
  UnknownError = 'unknown_error',
  UnknownFileType = 'unknown_file_type'
}
