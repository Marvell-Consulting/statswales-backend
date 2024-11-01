import { FindOneOptions, FindOptionsRelations } from 'typeorm';
import { has } from 'lodash';

import { dataSource } from '../db/data-source';
import { Dataset } from '../entities/dataset/dataset';
import { DatasetInfo } from '../entities/dataset/dataset-info';
import { User } from '../entities/user/user';
import { logger } from '../utils/logger';
import { DatasetListItemDTO } from '../dtos/dataset-list-item-dto';
import { Locale } from '../enums/locale';
import { DatasetInfoDTO } from '../dtos/dataset-info-dto';

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
    async getById(id: string, relations: FindOptionsRelations<Dataset> = defaultRelations): Promise<Dataset> {
        const findOptions: FindOneOptions<Dataset> = { where: { id }, relations };

        if (has(relations, 'revisions.imports.sources')) {
            findOptions.order = { revisions: { imports: { sources: { columnIndex: 'ASC' } } } };
        }

        return this.findOneOrFail(findOptions);
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

    async patchInfoById(datasetId: string, infoDto: DatasetInfoDTO): Promise<Dataset> {
        const infoRepo = dataSource.getRepository(DatasetInfo);
        const existingInfo = await infoRepo.findOne({ where: { id: datasetId, language: infoDto.language } });

        if (existingInfo) {
            await infoRepo.merge(existingInfo, infoDto).save();
        } else {
            await infoRepo.create({ dataset: { id: datasetId }, ...infoDto }).save();
        }

        return this.getById(datasetId);
    },

    async listAllByLanguage(lang: Locale): Promise<DatasetListItemDTO[]> {
        const qb = this.createQueryBuilder('d')
            .select(['d.id as id', 'di.title as title'])
            .innerJoin('d.datasetInfo', 'di')
            .where('di.language LIKE :lang', { lang: `${lang}%` })
            .groupBy('d.id, di.title')
            .orderBy('d.createdAt', 'ASC');

        return qb.getRawMany();
    },

    async listActiveByLanguage(lang: Locale): Promise<DatasetListItemDTO[]> {
        const qb = this.createQueryBuilder('d')
            .select(['d.id as id', 'di.title as title'])
            .innerJoin('d.datasetInfo', 'di')
            .innerJoin('d.revisions', 'r')
            .innerJoin('r.imports', 'i')
            .where('di.language LIKE :lang', { lang: `${lang}%` })
            .groupBy('d.id, di.title')
            .orderBy('d.createdAt', 'ASC');

        return qb.getRawMany();
    }
});
