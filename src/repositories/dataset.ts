import { FindOneOptions, FindOptionsRelations, IsNull, Not } from 'typeorm';
import { has, set } from 'lodash';

import { logger } from '../utils/logger';
import { dataSource } from '../db/data-source';
import { Dataset } from '../entities/dataset/dataset';
import { DatasetListItemDTO } from '../dtos/dataset-list-item-dto';
import { Locale } from '../enums/locale';
import { Team } from '../entities/user/team';
import { Revision } from '../entities/dataset/revision';
import { ResultsetWithCount } from '../interfaces/resultset-with-count';
import { DataTable } from '../entities/dataset/data-table';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { FactTableColumnType } from '../enums/fact-table-column-type';

export const datasetAll: FindOptionsRelations<Dataset> = {
    createdBy: true,
    factTable: true,
    dimensions: { metadata: true, lookupTable: true },
    measure: { lookupTable: true, measureTable: true },
    revisions: {
        createdBy: true,
        metadata: true,
        revisionProviders: true,
        revisionTopics: true,
        dataTable: { dataTableDescriptions: true }
    }
};

export const datasetDraftWithMetadata: FindOptionsRelations<Dataset> = {
    draftRevision: { metadata: true }
};

export const datasetDraftWithDataTable: FindOptionsRelations<Dataset> = {
    factTable: true,
    dimensions: { metadata: true, lookupTable: true },
    draftRevision: { dataTable: { dataTableDescriptions: true } }
};

export const datasetTasklistState: FindOptionsRelations<Dataset> = {
    draftRevision: { metadata: true, dataTable: true, revisionProviders: true, revisionTopics: true },
    dimensions: { metadata: true },
    measure: { measureTable: true },
    team: true
};

export const DatasetRepository = dataSource.getRepository(Dataset).extend({
    async getById(id: string, relations: FindOptionsRelations<Dataset> = {}): Promise<Dataset> {
        const findOptions: FindOneOptions<Dataset> = { where: { id }, relations };

        if (has(relations, 'factTable')) {
            set(findOptions, 'order.factTable', { columnIndex: 'ASC' });
        }

        if (has(relations, 'revisions.dataTable.dataTableDescriptions')) {
            set(findOptions, 'revisions.dataTable.dataTableDescriptions', { columnIndex: 'ASC' });
        }

        return this.findOneOrFail(findOptions);
    },

    async getPublishedById(id: string): Promise<Dataset> {
        const findOptions: FindOneOptions<Dataset> = {
            where: { id, live: Not(IsNull()) },
            relations: datasetAll,
            order: {
                dimensions: { metadata: { language: 'ASC' } },
                revisions: { dataTable: { dataTableDescriptions: { columnIndex: 'ASC' } } }
            }
        };

        return this.findOneOrFail(findOptions);
    },

    async deleteById(id: string): Promise<void> {
        await this.delete({ id });
    },

    async replaceFactTable(dataset: Dataset, dataTable: DataTable): Promise<void> {
        if (dataset.factTable && dataset.factTable.length > 0) {
            logger.debug(`Existing factTable found for dataset ${dataset.id}, deleting`);
            await dataSource.getRepository(FactTableColumn).remove(dataset.factTable);
        }

        logger.debug(`Creating fact table definitions for dataset ${dataset.id}`);

        const factColumns: FactTableColumn[] = [];

        dataTable.dataTableDescriptions.map((dataTableCol) => {
            const factTableColumn = FactTableColumn.create({
                id: dataset.id,
                columnName: dataTableCol.columnName,
                columnIndex: dataTableCol.columnIndex,
                columnDatatype: dataTableCol.columnDatatype,
                columnType: FactTableColumnType.Unknown
            });

            factColumns.push(factTableColumn);
        });

        await dataSource.getRepository(FactTableColumn).save(factColumns);
    },

    async listByLanguage(lang: Locale, page: number, limit: number): Promise<ResultsetWithCount<DatasetListItemDTO>> {
        // TODO: statuses are a best approximation for a first pass
        const qb = this.createQueryBuilder('d')
            .select(['d.id as id', 'r.title as title', 'r.updated_at as last_updated'])
            .addSelect(
                `
                CASE
                    WHEN d.live IS NOT NULL AND d.live < NOW() THEN 'live'
                    ELSE 'new'
                END`,
                'status'
            )
            .addSelect(
                `
                CASE
                    WHEN d.live IS NOT NULL AND d.live < NOW() AND r.approved_at IS NOT NULL AND r.publish_at < NOW() THEN 'published'
                    WHEN d.live IS NOT NULL AND d.live < NOW() AND r.approved_at IS NOT NULL AND r.publish_at > NOW() THEN 'update_scheduled'
                    WHEN d.live IS NOT NULL AND d.live > NOW() AND r.approved_at IS NOT NULL AND r.publish_at > NOW() THEN 'scheduled'
                    WHEN d.live IS NOT NULL AND d.live < NOW() AND r.approved_at IS NULL THEN 'update_incomplete'
                    WHEN d.live IS NULL AND r.approved_at IS NULL THEN 'incomplete'
                    ELSE 'incomplete'
                END
            `,
                'publishing_status'
            )
            .innerJoin(
                (subQuery) => {
                    // only join the latest revision for each dataset
                    return subQuery
                        .select('DISTINCT ON (rev.dataset_id) rev.*, rm.title')
                        .from(Revision, 'rev')
                        .innerJoin('rev.metadata', 'rm')
                        .where('rm.language LIKE :lang', { lang: `${lang}%` })
                        .orderBy('rev.dataset_id')
                        .addOrderBy('rev.created_at', 'DESC');
                },
                'r',
                'r.dataset_id = d.id'
            )
            .groupBy('d.id, r.title, r.updated_at, r.approved_at, r.publish_at');

        const offset = (page - 1) * limit;

        const countQuery = qb.clone();
        const resultQuery = qb.orderBy('r.updated_at', 'DESC').offset(offset).limit(limit);
        const [data, count] = await Promise.all([resultQuery.getRawMany(), countQuery.getCount()]);

        return { data, count };
    },

    async listPublishedByLanguage(
        lang: Locale,
        page: number,
        limit: number
    ): Promise<ResultsetWithCount<DatasetListItemDTO>> {
        const qb = this.createQueryBuilder('d')
            .select(['d.id as id', 'rm.title as title', 'd.live as published_date'])
            .innerJoin('d.publishedRevision', 'r')
            .innerJoin('r.metadata', 'rm')
            .where('rm.language LIKE :lang', { lang: `${lang}%` })
            .andWhere('d.live IS NOT NULL')
            .andWhere('d.live < NOW()')
            .groupBy('d.id, rm.title, d.live')
            .orderBy('d.live', 'DESC');

        const offset = (page - 1) * limit;
        const countQuery = qb.clone();
        const resultQuery = qb.orderBy('d.live', 'DESC').offset(offset).limit(limit);
        const [data, count] = await Promise.all([resultQuery.getRawMany(), countQuery.getCount()]);

        return { data, count };
    },

    // async addDatasetProvider(datasetId: string, dataProvider: RevisionProviderDTO): Promise<Dataset> {
    //     const newProvider = RevisionProviderDTO.toDatsetProvider(dataProvider);

    //     // add new data provider for both languages
    //     const altLang = newProvider.language.includes(Locale.English) ? Locale.WelshGb : Locale.EnglishGb;

    //     const newProviderAltLang: Partial<RevisionProvider> = {
    //         ...newProvider,
    //         id: undefined,
    //         language: altLang.toLowerCase()
    //     };

    //     await dataSource.getRepository(RevisionProvider).save([newProvider, newProviderAltLang]);

    //     logger.debug(`Added new provider for dataset ${datasetId}`);

    //     return this.getById(datasetId, { datasetProviders: { provider: true, providerSource: true } });
    // },

    // async updateDatasetProviders(datasetId: string, dataProviders: RevisionProviderDTO[]): Promise<Dataset> {
    //     const existing = await dataSource.getRepository(RevisionProvider).findBy({ datasetId });
    //     const submitted = dataProviders.map((provider) => RevisionProviderDTO.toDatsetProvider(provider));

    //     // we can receive updates in a single language, but we need to update the relations for both languages

    //     // work out what providers have been removed and remove for both languages
    //     const toRemove = existing.filter((existing) => {
    //         // if the group id is still present in the submitted data then don't remove those providers
    //         return !submitted.some((submitted) => submitted.groupId === existing.groupId);
    //     });

    //     await dataSource.getRepository(RevisionProvider).remove(toRemove);

    //     // update the data providers for both languages
    //     const toUpdate = existing
    //         .filter((existing) => submitted.some((submitted) => submitted.groupId === existing.groupId))
    //         .map((updating) => {
    //             const updated = submitted.find((submitted) => submitted.groupId === updating.groupId)!;
    //             updating.providerId = updated.providerId;
    //             updating.providerSourceId = updated.providerSourceId;
    //             return updating;
    //         });

    //     await dataSource.getRepository(RevisionProvider).save(toUpdate);

    //     logger.debug(
    //         `Removed ${toRemove.length} providers and updated ${toUpdate.length} providers for dataset ${datasetId}`
    //     );

    //     return this.getById(datasetId, { datasetProviders: { provider: true, providerSource: true } });
    // },

    // async updateDatasetTopics(datasetId: string, topics: string[]): Promise<Dataset> {
    //     // remove any existing topic relations
    //     const existing = await dataSource.getRepository(RevisionTopic).findBy({ datasetId });
    //     await dataSource.getRepository(RevisionTopic).remove(existing);

    //     // save the new topic relations
    //     const datasetTopics = topics.map((topicId: string) => {
    //         return dataSource.getRepository(RevisionTopic).create({ datasetId, topicId: parseInt(topicId, 10) });
    //     });

    //     await dataSource.getRepository(RevisionTopic).save(datasetTopics);

    //     return this.getById(datasetId, {});
    // },

    async updateDatasetTeam(datasetId: string, teamId: string): Promise<Dataset> {
        const dataset = await this.findOneOrFail({ where: { id: datasetId } });
        const team = await dataSource.getRepository(Team).findOneByOrFail({ id: teamId });
        dataset.team = team;
        await dataset.save();
        return this.getById(datasetId, {});
    }
});
