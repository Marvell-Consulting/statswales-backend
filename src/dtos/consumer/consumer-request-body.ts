import { Locale } from '../../enums/locale';

export interface ConsumerRequestBody {
  pivot?: {
    backend?: 'postgres' | 'duckdb'; // Default: 'duckdb'
    include_performance?: boolean; // Default: false
    x: string | string[];
    y: string | string[];
  };
  language: Locale;
  sort_by: string[];
  filters?: unknown[]; // Turn this in to a map<string, string[]> in code
  options: {
    use_raw_column_names?: boolean; // Default: true
    use_reference_values?: boolean; // Default: true
    data_value_type?: 'raw' | 'formatted' | 'with_note_codes';
  };
}
