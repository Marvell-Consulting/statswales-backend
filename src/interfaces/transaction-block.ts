import { Locale } from '../enums/locale';
import { BuildStage } from '../enums/build-stage';

export interface TransactionBlock {
  buildStage: BuildStage;
  statements: string[];
  indexColumns?: Map<Locale, string[]>;
}
