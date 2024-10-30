import { last } from 'lodash';
import { FindOptionsRelations } from 'typeorm';

import { dataSource } from '../db/data-source';
import { Dataset } from '../entities/dataset/dataset';
import { DatasetInfo } from '../entities/dataset/dataset-info';
import { FileImport } from '../entities/dataset/file-import';
import { Revision } from '../entities/dataset/revision';
import { User } from '../entities/user/user';
import { logger } from '../utils/logger';
import { DatasetListItemDTO } from '../dtos/dataset-list-item-dto';
import { Locale } from '../enums/locale';

const defaultRelations: FindOptionsRelations<Dataset> = {
    createdBy: true,
    datasetInfo: true,
    dimensions: {
        dimensionInfo: true
    },
    revisions: {
        createdBy: true,
        imports: {
            sources: true
        }
    }
};

export const DatasetRepository = dataSource.getRepository(Dataset).extend({
    async getById(id: string): Promise<Dataset> {
        return this.findOneOrFail({
            where: { id },
            relations: defaultRelations,
            order: { revisions: { imports: { sources: { columnIndex: 'ASC' } } } }
        });
    },

    async deleteById(id: string): Promise<void> {
        await this.delete({ id });
    },

    async createWithTitle(user: User, language?: string, title?: string): Promise<Dataset> {
        logger.debug(`Creating new Dataset...`);
        const dataset = await this.create({ createdBy: user }).save();

        if (language && title) {
            logger.debug(`Creating new DatasetInfo with language "${language}" and title "${title}"...`);
            const datasetInfo = await dataSource.getRepository(DatasetInfo).create({ dataset, language, title }).save();
            dataset.datasetInfo = [datasetInfo];
        }

        return this.getById(dataset.id);
    },

    async createRevisionFromImport(dataset: Dataset, fileImport: FileImport, user: User): Promise<Dataset> {
        logger.debug(`Creating new Revision for Dataset "${dataset.id}" from Import "${fileImport.id}"...`);

        const existingRevisions = await dataSource
            .getRepository(Revision)
            .find({ where: { dataset: { id: dataset.id } }, order: { revisionIndex: 'ASC' } });

        const revisionIndex = existingRevisions.length + 1;
        const previousRevision = last(existingRevisions);

        await Revision.create({
            dataset,
            revisionIndex,
            previousRevision,
            imports: [fileImport],
            createdBy: user
        }).save();

        return this.getById(dataset.id);
    },

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
    },

    async listAllByLanguage(lang: Locale): Promise<DatasetListItemDTO[]> {
        const qb = this.createQueryBuilder('d')
            .select(['d.id as id', 'di.title as title'])
            .innerJoin('d.datasetInfo', 'di')
            .where('di.language LIKE :lang', { lang: `${lang}%` })
            .groupBy('d.id, di.title');

        return qb.getRawMany();
    },

    async listActiveByLanguage(lang: Locale): Promise<DatasetListItemDTO[]> {
        const qb = this.createQueryBuilder('d')
            .select(['d.id as id', 'di.title as title'])
            .innerJoin('d.datasetInfo', 'di')
            .innerJoin('d.revisions', 'r')
            .innerJoin('r.imports', 'i')
            .where('di.language LIKE :lang', { lang: `${lang}%` })
            .groupBy('d.id, di.title');

        return qb.getRawMany();
    }
});
