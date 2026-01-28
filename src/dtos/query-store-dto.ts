import { DataOptionsDTO } from './data-options-dto';
import { FactTableToDimensionName } from '../interfaces/fact-table-column-to-dimension-name';
import { QueryStore } from '../entities/query-store';

export class QueryStoreDto {
  id: string;
  hash: string;
  datasetId: string;
  revisionId: string;
  requestObject: DataOptionsDTO;
  query: Record<string, string>;
  totalLines: number;
  columnMapping: FactTableToDimensionName[];
  createdAt: Date;
  updatedAt: Date;

  static fromQueryStore(queryStore: QueryStore): QueryStoreDto {
    const dto = new QueryStoreDto();
    dto.id = queryStore.id;
    dto.hash = queryStore.hash;
    dto.datasetId = queryStore.datasetId;
    dto.revisionId = queryStore.revisionId;
    dto.requestObject = queryStore.requestObject;
    dto.query = queryStore.query;
    dto.totalLines = queryStore.totalLines;
    dto.columnMapping = queryStore.columnMapping;
    dto.createdAt = queryStore.createdAt;
    dto.updatedAt = queryStore.updatedAt;
    return dto;
  }
}
