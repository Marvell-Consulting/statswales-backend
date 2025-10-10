import { FactTableColumn } from '../entities/dataset/fact-table-column';

export interface FactTableInfo {
  factTableCreationQuery: string;
  measureColumn?: FactTableColumn;
  notesCodeColumn?: FactTableColumn;
  dataValuesColumn?: FactTableColumn;
  factTableDef: string[];
  factIdentifiers: FactTableColumn[];
  compositeKey: string[];
}
