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
import { DatasetProviderDTO } from '../dtos/dataset-provider-dto';
import { DatasetProvider } from '../entities/dataset/dataset-provider';

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
    },
    datasetProviders: {
        provider: true,
        providerSource: true
    }
};

export const DatasetRepository = dataSource.getRepository(Dataset).extend({
    async getById(id: string, relations: FindOptionsRelations<Dataset> = defaultRelations): Promise<Dataset> {
        const findOptions: FindOneOptions<Dataset> = { where: { id }, relations };

        if (has(relations, 'revisions.imports.sources')) {
            // sort sources by column index if they're requested
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
        const updatedInfo = DatasetInfoDTO.toDatasetInfo(infoDto);

        if (existingInfo) {
            await infoRepo.merge(existingInfo, updatedInfo).save();
        } else {
            await infoRepo.create({ dataset: { id: datasetId }, ...updatedInfo }).save();
        }

        return this.getById(datasetId);
    },

    async listAllByLanguage(lang: Locale): Promise<DatasetListItemDTO[]> {
        const qb = this.createQueryBuilder('d')
            .select(['d.id as id', 'di.title as title'])
            .innerJoin('d.datasetInfo', 'di')
            .where('di.language ILIKE :lang', { lang: `${lang}%` })
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
            .where('di.language ILIKE :lang', { lang: `${lang}%` })
            .groupBy('d.id, di.title')
            .orderBy('d.createdAt', 'ASC');

        return qb.getRawMany();
    },

    async addDatasetProvider(datasetId: string, dataProvider: DatasetProviderDTO): Promise<Dataset> {
        const newProvider = DatasetProviderDTO.toDatsetProvider(dataProvider);
        const altLang = newProvider.language.includes(Locale.English) ? Locale.WelshGb : Locale.EnglishGb;

        // ad new data provider for both languages
        const newProviderAltLang: Partial<DatasetProvider> = {
            ...newProvider,
            id: undefined,
            language: altLang.toLowerCase()
        };

        await dataSource.getRepository(DatasetProvider).save([newProvider, newProviderAltLang]);

        return this.getById(datasetId);
    },

    async updateDatasetProviders(datasetId: string, dataProviders: DatasetProviderDTO[]): Promise<Dataset> {
        const existing = await dataSource.getRepository(DatasetProvider).findBy({ datasetId });

        // we only receive updates in a single language, but we need to update the relations for both languages
        // provider id will be the same for both languages, so we can filter on provider id
        const submitted = dataProviders.map((provider) => DatasetProviderDTO.toDatsetProvider(provider));

        // work out what providers have been removed and remove for both languages
        const toRemove = existing.filter((existing) => {
            // if the group id is still present in the submitted data then don't remove those providers
            return !submitted.some((submitted) => submitted.groupId === existing.groupId);
        });

        await dataSource.getRepository(DatasetProvider).remove(toRemove);

        // update the data providers for both languages
        const toUpdate = existing
            .filter((existing) => submitted.some((submitted) => submitted.groupId === existing.groupId))
            .map((updating) => {
                const updated = submitted.find((submitted) => submitted.groupId === updating.groupId)!;
                updating.providerId = updated.providerId;
                updating.providerSourceId = updated.providerSourceId;
                return updating;
            });

        await dataSource.getRepository(DatasetProvider).save(toUpdate);

        logger.debug(
            `Removed ${toRemove.length} providers and updated ${toUpdate.length} providers for dataset ${datasetId}`
        );

        return this.getById(datasetId);
    }
});
