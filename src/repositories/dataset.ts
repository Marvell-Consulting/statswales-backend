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
import { DatasetTopic } from '../entities/dataset/dataset-topic';
import { Team } from '../entities/user/team';
import { TranslationDTO } from '../dtos/translations-dto';
import { DimensionInfo } from '../entities/dataset/dimension-info';
import { Revision } from '../entities/dataset/revision';
import { ResultsetWithCount } from '../interfaces/resultset-with-count';

const defaultRelations: FindOptionsRelations<Dataset> = {
    createdBy: true,
    datasetInfo: true,
    dimensions: {
        dimensionInfo: true,
        lookupTable: true
    },
    measure: {
        lookupTable: true,
        measureInfo: true
    },
    revisions: {
        createdBy: true,
        factTables: {
            factTableInfo: true
        }
    },
    datasetProviders: {
        provider: true,
        providerSource: true
    },
    datasetTopics: {
        topic: true
    },
    team: {
        info: true,
        organisation: {
            info: true
        }
    }
};

export const DatasetRepository = dataSource.getRepository(Dataset).extend({
    async getById(id: string, relations: FindOptionsRelations<Dataset> = defaultRelations): Promise<Dataset> {
        const findOptions: FindOneOptions<Dataset> = { where: { id }, relations };

        if (has(relations, 'revisions.factTables.factTableInfo')) {
            // sort sources by column index if they're requested
            findOptions.order = {
                dimensions: { dimensionInfo: { language: 'ASC' } },
                revisions: { factTables: { factTableInfo: { columnIndex: 'ASC' } } }
            };
        }

        return this.findOneOrFail(findOptions);
    },

    async deleteById(id: string): Promise<void> {
        await this.delete({ id });
    },

    async createWithTitle(user: User, language: Locale, title: string): Promise<Dataset> {
        logger.debug(`Creating new Dataset...`);
        const dataset = await this.create({ createdBy: user }).save();
        const altLang = language.includes('en') ? Locale.WelshGb : Locale.EnglishGb;

        logger.debug(`Creating new DatasetInfo with language "${language}" and title "${title}"...`);
        const datasetInfo = await dataSource.getRepository(DatasetInfo).create({ dataset, language, title }).save();
        const altLangDatasetInfo = await dataSource
            .getRepository(DatasetInfo)
            .create({ dataset, language: altLang })
            .save();

        dataset.datasetInfo = [datasetInfo, altLangDatasetInfo];

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

    async listActiveByLanguage(
        lang: Locale,
        page: number,
        limit: number
    ): Promise<ResultsetWithCount<DatasetListItemDTO>> {
        // TODO: statuses are a best approximation for a first pass
        // they will need to be revisited once updates are a thing
        const qb = this.createQueryBuilder('d')
            .select(['d.id as id', 'di.title as title', 'di.updatedAt as last_updated'])
            .addSelect("CASE WHEN d.live IS NOT NULL THEN 'live' ELSE 'new' END", 'status')
            .addSelect(
                `
                CASE
                    WHEN d.live IS NOT NULL AND r.approved_at IS NOT NULL AND r.publish_at <= NOW() THEN 'published'
                    WHEN r.approved_at IS NOT NULL THEN 'scheduled'
                    ELSE 'incomplete'
                END
            `,
                'publishing_status'
            )
            .innerJoin('d.datasetInfo', 'di')
            .innerJoin(
                (subQuery) => {
                    // only join the latest revision for each dataset
                    return subQuery
                        .select('DISTINCT ON (dataset_id) *')
                        .from(Revision, 'rev')
                        .orderBy('dataset_id')
                        .addOrderBy('created_at', 'DESC');
                },
                'r',
                'r.dataset_id = d.id'
            )
            .where('di.language LIKE :lang', { lang: `${lang}%` })
            .groupBy('d.id, di.title, di.updatedAt, r.approved_at, r.publish_at');

        const offset = (page - 1) * limit;

        const countQuery = qb.clone();
        const resultQuery = qb.orderBy('di.updatedAt', 'DESC').offset(offset).limit(limit);
        const [data, count] = await Promise.all([resultQuery.getRawMany(), countQuery.getCount()]);

        return { data, count };
    },

    async listPublishedByLanguage(
        lang: Locale,
        page: number,
        limit: number
    ): Promise<ResultsetWithCount<DatasetListItemDTO>> {
        const qb = this.createQueryBuilder('d')
            .select(['d.id as id', 'di.title as title', 'd.live as published_date'])
            .innerJoin('d.datasetInfo', 'di')
            .where('di.language LIKE :lang', { lang: `${lang}%` })
            .andWhere('d.live IS NOT NULL')
            .groupBy('d.id, di.title, d.live');

        const offset = (page - 1) * limit;
        const countQuery = qb.clone();
        const resultQuery = qb.orderBy('d.live', 'DESC').offset(offset).limit(limit);
        const [data, count] = await Promise.all([resultQuery.getRawMany(), countQuery.getCount()]);

        return { data, count };
    },

    async addDatasetProvider(datasetId: string, dataProvider: DatasetProviderDTO): Promise<Dataset> {
        const newProvider = DatasetProviderDTO.toDatsetProvider(dataProvider);

        // add new data provider for both languages
        const altLang = newProvider.language.includes(Locale.English) ? Locale.WelshGb : Locale.EnglishGb;

        const newProviderAltLang: Partial<DatasetProvider> = {
            ...newProvider,
            id: undefined,
            language: altLang.toLowerCase()
        };

        await dataSource.getRepository(DatasetProvider).save([newProvider, newProviderAltLang]);

        logger.debug(`Added new provider for dataset ${datasetId}`);

        return this.getById(datasetId);
    },

    async updateDatasetProviders(datasetId: string, dataProviders: DatasetProviderDTO[]): Promise<Dataset> {
        const existing = await dataSource.getRepository(DatasetProvider).findBy({ datasetId });
        const submitted = dataProviders.map((provider) => DatasetProviderDTO.toDatsetProvider(provider));

        // we can receive updates in a single language, but we need to update the relations for both languages

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
    },

    async updateDatasetTopics(datasetId: string, topics: string[]): Promise<Dataset> {
        // remove any existing topic relations
        const existing = await dataSource.getRepository(DatasetTopic).findBy({ datasetId });
        await dataSource.getRepository(DatasetTopic).remove(existing);

        // save the new topic relations
        const datasetTopics = topics.map((topicId: string) => {
            return dataSource.getRepository(DatasetTopic).create({ datasetId, topicId: parseInt(topicId, 10) });
        });

        await dataSource.getRepository(DatasetTopic).save(datasetTopics);

        return this.getById(datasetId);
    },

    async updateDatasetTeam(datasetId: string, teamId: string): Promise<Dataset> {
        const dataset = await this.findOneOrFail({ where: { id: datasetId } });
        const team = await dataSource.getRepository(Team).findOneByOrFail({ id: teamId });
        dataset.team = team;
        await dataset.save();
        return this.getById(datasetId);
    },

    async updateTranslations(datasetId: string, translations: TranslationDTO[]): Promise<Dataset> {
        const dataset = await this.findOneOrFail({ where: { id: datasetId } });
        const dimensionInfoRepo = dataSource.getRepository(DimensionInfo);
        const infoRepo = dataSource.getRepository(DatasetInfo);

        const dimensionTranslations = translations.filter((t) => t.type === 'dimension');

        logger.debug(`Updating dimension names...`);

        for (const row of dimensionTranslations) {
            const englishDimInfo = await dimensionInfoRepo.findOneByOrFail({ id: row.id, language: Locale.EnglishGb });
            englishDimInfo.name = row.english || '';
            await englishDimInfo.save();

            const welshDimInfo = await dimensionInfoRepo.findOneByOrFail({ id: row.id, language: Locale.WelshGb });
            welshDimInfo.name = row.cymraeg || '';
            await welshDimInfo.save();
        }

        const metaTranslations = translations.filter((t) => t.type === 'metadata');

        logger.debug(`Updating metadata...`);

        const englishInfo =
            (await infoRepo.findOneBy({ dataset, language: Locale.EnglishGb })) ||
            infoRepo.create({ dataset, language: Locale.EnglishGb });

        const welshInfo =
            (await infoRepo.findOneBy({ dataset, language: Locale.WelshGb })) ||
            infoRepo.create({ ...englishInfo, language: Locale.WelshGb });

        metaTranslations.forEach((row) => {
            const metaKey = row.key as 'title' | 'description' | 'collection' | 'quality' | 'roundingDescription';
            englishInfo[metaKey] = row.english || '';
            welshInfo[metaKey] = row.cymraeg || '';
        });

        await englishInfo.save();
        await welshInfo.save();

        return this.getById(datasetId);
    }
});
