export enum CubeBuildStatus {
  Queued = 'queued',
  Building = 'building',
  Failed = 'failed',
  SchemaRename = 'schema_rename',
  Materializing = 'materializing',
  Completed = 'completed'
}
