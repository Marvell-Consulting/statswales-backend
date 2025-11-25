export enum BuildStage {
  BaseTables = 'base_tables',
  FactTable = 'fact_table',
  Measure = 'measure',
  Dimensions = 'dimensions',
  NoteCodes = 'note_codes',
  CoreView = 'core_view',
  ValidationTableBuild = 'validation_table_build',
  PostBuildMetadata = 'post_build_metadata',
  ViewMaterialisation = 'view_materialisation'
}
