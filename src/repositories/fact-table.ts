import { dataSource } from '../db/data-source';
import { logger } from '../utils/logger';
import { FactTable } from '../entities/dataset/fact-table';
import { Revision } from '../entities/dataset/revision';

export const FactTableRepository = dataSource.getRepository(Revision).extend({
    async getFactTableById(datasetId: string, revisionId: string, factTableId: string): Promise<FactTable> {
        return dataSource.getRepository(FactTable).findOneOrFail({
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
                factTableInfo: true
            }
        });
    }
});
