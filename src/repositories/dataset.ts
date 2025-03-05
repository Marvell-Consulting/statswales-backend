import { FindOneOptions, FindOptionsRelations } from 'typeorm';
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
import { PeriodCovered } from '../interfaces/period-covered';

export const withAll: FindOptionsRelations<Dataset> = {
    createdBy: true,
    factTable: true,
    dimensions: { metadata: true, lookupTable: true },
    measure: { lookupTable: true, measureTable: true },
    draftRevision: {
        metadata: true,
        dataTable: true,
        revisionProviders: { provider: true },
        revisionTopics: { topic: true }
    },
    revisions: true
};

export const withDraftAndMetadata: FindOptionsRelations<Dataset> = {
    draftRevision: { metadata: true }
};

export const withDraftAndProviders: FindOptionsRelations<Dataset> = {
    draftRevision: { revisionProviders: { provider: true, providerSource: true } }
};

export const withDraftAndTopics: FindOptionsRelations<Dataset> = {
    draftRevision: { revisionTopics: { topic: true } }
};

export const withDraftForCube: FindOptionsRelations<Dataset> = {
    factTable: true,
    draftRevision: { dataTable: { dataTableDescriptions: true } },
    dimensions: { metadata: true, lookupTable: true },
    measure: { metadata: true, measureTable: true, lookupTable: true }
};

export const withDraftForTasklistState: FindOptionsRelations<Dataset> = {
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
            .select(['d.id as id', 'r.title as title', 'r.title_alt as title_alt', 'r.updated_at as last_updated'])
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
                        .select('DISTINCT ON (rev.dataset_id) rev.*, rm1.title as title, rm2.title as title_alt')
                        .from(Revision, 'rev')
                        .innerJoin('rev.metadata', 'rm1', 'rm1.language = :lang', { lang })
                        .innerJoin('rev.metadata', 'rm2', 'rm2.language != :lang', { lang })
                        .orderBy('rev.dataset_id')
                        .addOrderBy('rev.created_at', 'DESC');
                },
                'r',
                'r.dataset_id = d.id'
            )
            .groupBy('d.id, r.title, r.title_alt, r.updated_at, r.approved_at, r.publish_at');

        const offset = (page - 1) * limit;

        const countQuery = qb.clone();
        const resultQuery = qb.orderBy('r.updated_at', 'DESC').offset(offset).limit(limit);
        const [data, count] = await Promise.all([resultQuery.getRawMany(), countQuery.getCount()]);

        return { data, count };
    },

    async updateDatasetTeam(datasetId: string, teamId: string): Promise<Dataset> {
        const dataset = await this.findOneOrFail({ where: { id: datasetId } });
        const team = await dataSource.getRepository(Team).findOneByOrFail({ id: teamId });
        dataset.team = team;
        await dataset.save();
        return this.getById(datasetId, {});
    },

    async publish(revision: Revision, period: PeriodCovered): Promise<Dataset> {
        const dataset = revision.dataset;

        dataset.startDate = period.start_date;
        dataset.endDate = period.end_date;
        dataset.draftRevision = null;
        dataset.publishedRevision = revision;

        if (revision.revisionIndex === 1) {
            dataset.live = revision.publishAt; // set the first published date if this is the first rev
        }

        return DatasetRepository.save(dataset);
    },

    async withdraw(revision: Revision): Promise<Dataset> {
        const dataset = revision.dataset;

        dataset.draftRevision = revision;
        dataset.publishedRevision = revision.previousRevision;

        return DatasetRepository.save(dataset);
    }
});
