export interface CubeViewConfig {
  name: string;
  dataValues: 'formatted' | 'annotated' | 'raw';
  refcodes: boolean;
  dates: 'formatted' | 'raw' | 'none';
  hierarchies: boolean;
  sort_orders: boolean;
  note_descriptions: boolean;
}
