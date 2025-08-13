import { CSVHeader } from '../dtos/view-dto';
import { Dataset } from '../entities/dataset/dataset';
import { DimensionType } from '../enums/dimension-type';
import { FactTableColumnType } from '../enums/fact-table-column-type';

export const getColumnHeaders = (
  dataset: Dataset,
  columns: string[],
  filters: Record<string, string>[]
): CSVHeader[] => {
  return columns.map((columnName, idx) => {
    let source_type = FactTableColumnType.Unknown;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let extractor: any;

    // we can use the filters to map translated dimension name to fact table column
    const filter = filters.find((filter) => filter.dimension_name === columnName);

    if (filter) {
      const factTableColumn = dataset.factTable?.find((factCol) => factCol.columnName === filter.fact_table_column);

      if (factTableColumn?.columnType === FactTableColumnType.Dimension) {
        const dimension = dataset.dimensions?.find((dim) => dim.factTableColumn === factTableColumn.columnName);
        if (dimension?.type === DimensionType.Date || dimension?.type === DimensionType.Time) {
          source_type = FactTableColumnType.Time;
          extractor = dimension.extractor;
        } else {
          source_type = FactTableColumnType.Dimension;
        }
      }

      if (factTableColumn?.columnType === FactTableColumnType.Measure) {
        source_type = FactTableColumnType.Measure;
      }
    } else {
      source_type = FactTableColumnType.DataValues;
    }

    return { index: idx, name: columnName, source_type, extractor };
  });
};
