import { CubeViewConfig } from './cube-view-config';
import { Locale } from '../enums/locale';

export interface CubeViewBuilder {
  name: string;
  config: CubeViewConfig;
  columns: Map<Locale, Set<string>>;
}
