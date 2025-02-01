import { dataSource } from '../db/data-source';
import { logger } from '../utils/logger';
import { DataTable } from '../entities/dataset/data-table';
import { Revision } from '../entities/dataset/revision';

export const FactTableRepository = dataSource.getRepository(Revision).extend({
    async getFactTableById(datasetId: string, revisionId: string, factTableId: string): Promise<DataTable> {
        return dataSource.getRepository(DataTable).findOneOrFail({
            where: {
                id: factTableId,
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
