export interface ConsumerOptions {
  pivot?: {
    backend: 'postgres' | 'duckdb'; // Default: 'duckdb'
    include_performance: boolean; // Default: false
    x: string | string[];
    y: string | string[];
  };
  filters: Record<string, string[]>[];
  options: {
    use_raw_column_names?: boolean; // Default: true
    use_reference_values?: boolean; // Default: true
    data_value_type?: 'raw' | 'formatted' | 'with_note_codes';
  };
}
