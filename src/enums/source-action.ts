export enum SourceAction {
  Create = 'create',
  Append = 'append',
  Truncate = 'truncate_then_load',
  Ignore = 'ignore',
  Unknown = 'unknown'
}
