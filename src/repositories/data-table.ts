import { publisherDataSource } from '../db/publisher-source';
import { DataTable } from '../entities/dataset/data-table';

export const DataTableRepository = publisherDataSource.getRepository(DataTable).extend({
  async getDataTableById(datasetId: string, revisionId: string, dataTableId: string): Promise<DataTable> {
    return this.findOneOrFail({
      where: {
        id: dataTableId,
        revision: {
          id: revisionId,
          dataset: {
            id: datasetId
          }
        }
      },
      relations: {
        revision: {
          createdBy: true,
          dataset: {
            dimensions: true,
            measure: true
          }
        },
        dataTableDescriptions: true
      }
    });
  }
});
