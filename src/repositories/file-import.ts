import { dataSource } from '../db/data-source';
import { logger } from '../utils/logger';
import { FileImport } from '../entities/dataset/file-import';
import { Revision } from '../entities/dataset/revision';

export const FileImportRepository = dataSource.getRepository(Revision).extend({
    async getFileImportById(datasetId: string, revisionId: string, importId: string): Promise<FileImport> {
        logger.debug('Loading FileImport by datasetId, revisionId and importId...');

        const fileImport = await dataSource.getRepository(FileImport).findOneOrFail({
            where: {
                id: importId,
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
                        dimensions: true
                    }
                },
                sources: true
            }
        });

        return fileImport;
    }
});
