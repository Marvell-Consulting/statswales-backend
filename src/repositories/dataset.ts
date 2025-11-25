import { performance } from 'node:perf_hooks';

import { FindOneOptions, FindOptionsRelations, QueryBuilder, SelectQueryBuilder } from 'typeorm';
import { has, set } from 'lodash';

import { logger } from '../utils/logger';
import { dataSource } from '../db/data-source';
import { Dataset } from '../entities/dataset/dataset';
import { DatasetListItemDTO } from '../dtos/dataset-list-item-dto';
import { Locale } from '../enums/locale';
import { Revision } from '../entities/dataset/revision';
import { ResultsetWithCount } from '../interfaces/resultset-with-count';
import { DataTable } from '../entities/dataset/data-table';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { User } from '../entities/user/user';
import { getUserGroupIdsForUser } from '../utils/get-permissions-for-user';
import { DatasetStats } from '../interfaces/dashboard-stats';
import { CORE_VIEW_NAME } from '../services/cube-builder';

export const withStandardPreview: FindOptionsRelations<Dataset> = {
  createdBy: true,
  dimensions: { metadata: true },
  measure: { metadata: true },
  revisions: true,
  tasks: true, // needed for correct status badges
  publishedRevision: true // needed for correct status badges
};

export const withDeveloperPreview: FindOptionsRelations<Dataset> = {
  createdBy: true,
  factTable: true,
  dimensions: { metadata: true },
  measure: { metadata: true, measureTable: true, lookupTable: true },
  revisions: { metadata: true },
  tasks: true // needed for correct status badges
};

export const withLatestRevision: FindOptionsRelations<Dataset> = {
  endRevision: { metadata: true }
};

export const withFactTable: FindOptionsRelations<Dataset> = {
  factTable: true
};

export const withDraftAndMetadata: FindOptionsRelations<Dataset> = {
  draftRevision: { metadata: true },
  publishedRevision: true, // needed for correct status badges
  tasks: true // needed for correct status badges
};

export const withMetadataForTranslation: FindOptionsRelations<Dataset> = {
  draftRevision: { metadata: true },
  dimensions: { metadata: true },
  measure: { metadata: true }
};

export const withDraftAndProviders: FindOptionsRelations<Dataset> = {
  draftRevision: { revisionProviders: { provider: true, providerSource: true } }
};

export const withDraftAndTopics: FindOptionsRelations<Dataset> = {
  draftRevision: { revisionTopics: { topic: true } }
};

export const withDraftAndDataTable: FindOptionsRelations<Dataset> = {
  factTable: true,
  draftRevision: { metadata: true, dataTable: { dataTableDescriptions: true } }
};

export const withDraftAndMeasure: FindOptionsRelations<Dataset> = {
  draftRevision: { metadata: true },
  measure: { metadata: true, measureTable: true, lookupTable: true }
};

export const withDimensions: FindOptionsRelations<Dataset> = {
  dimensions: { metadata: true, lookupTable: true }
};

const listAllQuery = (qb: QueryBuilder<Dataset>, lang: Locale): SelectQueryBuilder<Dataset> => {
  return qb
    .select(['d.id AS id', 'r.title AS title', 'r.title_alt AS title_alt', 'r.updated_at AS last_updated_at'])
    .addSelect(`ugm.name AS group_name`)
    .addSelect(
      `
        CASE
          WHEN d.archived_at IS NOT NULL AND d.archived_at < NOW() THEN 'archived'
          WHEN pr.unpublished_at IS NOT NULL AND pr.unpublished_at < NOW() THEN 'offline'
          WHEN d.first_published_at IS NOT NULL AND d.first_published_at < NOW() THEN 'live'
          ELSE 'new'
        END`,
      'status'
    )
    .addSelect(
      `
        CASE
          WHEN d.first_published_at IS NOT NULL AND t.action = 'publish' AND t.status = 'requested' THEN 'update_pending_approval'
          WHEN t.action = 'publish' AND t.status = 'requested' THEN 'pending_approval'
          WHEN t.action = 'publish' AND t.status = 'rejected' THEN 'changes_requested'
          WHEN t.action = 'unpublish' AND t.status = 'requested' THEN 'unpublish_requested'
          WHEN t.action = 'archive' AND t.status = 'requested' THEN 'archive_requested'
          WHEN t.action = 'unarchive' AND t.status = 'requested' THEN 'unarchive_requested'
          WHEN pr.unpublished_at IS NOT NULL AND pr.unpublished_at < NOW() THEN 'unpublished'
          WHEN d.first_published_at IS NOT NULL AND d.first_published_at < NOW() AND r.approved_at IS NOT NULL AND r.publish_at < NOW() THEN 'published'
          WHEN d.first_published_at IS NOT NULL AND d.first_published_at < NOW() AND r.approved_at IS NOT NULL AND r.publish_at > NOW() THEN 'update_scheduled'
          WHEN d.first_published_at IS NOT NULL AND d.first_published_at > NOW() AND r.approved_at IS NOT NULL AND r.publish_at > NOW() THEN 'scheduled'
          WHEN d.first_published_at IS NOT NULL AND d.first_published_at < NOW() AND r.approved_at IS NULL THEN 'update_incomplete'
          WHEN d.first_published_at IS NULL AND r.approved_at IS NULL THEN 'incomplete'
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
    .leftJoin('d.publishedRevision', 'pr') // join published revision to check for unpublished flag for statuses (ie dataset taken offline)
    .innerJoin('d.userGroup', 'ug')
    .innerJoin('ug.metadata', 'ugm', 'ugm.language = :lang', { lang })
    .leftJoin('d.tasks', 't', 't.open = true')
    .groupBy(
      'd.id, r.title, ugm.name, r.title_alt, r.updated_at, r.approved_at, r.publish_at, pr.unpublished_at, t.action, t.status'
    );
};

export const DatasetRepository = dataSource.getRepository(Dataset).extend({
  async getById(id: string, relations: FindOptionsRelations<Dataset> = {}): Promise<Dataset> {
    const start = performance.now();
    const findOptions: FindOneOptions<Dataset> = { where: { id }, relations };

    if (has(relations, 'factTable')) {
      set(findOptions, 'order.factTable', { columnIndex: 'ASC' });
    }

    if (has(relations, 'revisions.dataTable.dataTableDescriptions')) {
      set(findOptions, 'revisions.dataTable.dataTableDescriptions', { columnIndex: 'ASC' });
    }

    const dataset = await this.findOneOrFail(findOptions);

    const end = performance.now();
    const size = Math.round(Buffer.byteLength(JSON.stringify(dataset)) / 1024);
    const time = Math.round(end - start);

    if (size > 50 || time > 200) {
      logger.warn(`LARGE/SLOW Dataset ${id} loaded { size: ${size}kb, time: ${time}ms }`);
    } else {
      logger.debug(`Dataset ${id} loaded { size: ${size}kb, time: ${time}ms }`);
    }

    return dataset;
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

  async listAll(
    locale: Locale,
    page: number,
    limit: number,
    search?: string
  ): Promise<ResultsetWithCount<DatasetListItemDTO>> {
    logger.debug(`Listing all datasets, language ${locale}, page ${page}, limit ${limit}`);
    const lang = locale.includes('en') ? Locale.EnglishGb : Locale.WelshGb;

    const query = listAllQuery(this.createQueryBuilder('d'), lang);
    query.leftJoin(User, 'u', 'r.created_by = u.id');
    query.addSelect(
      `
      CASE
        WHEN u.name IS NOT NULL THEN u.name
        ELSE u.email
      END
    `,
      'revision_by'
    );
    query.addGroupBy('u.id');

    if (search) {
      query.andWhere((qb) => {
        qb.where('r.title ILIKE :search', { search: `%${search}%` });
        qb.orWhere('d.id::text LIKE :search', { search: `${search}%` });
      });
    }

    const offset = (page - 1) * limit;
    const countQuery = query.clone();
    const resultQuery = query.orderBy('r.updated_at', 'DESC').offset(offset).limit(limit);
    const [data, count] = await Promise.all([resultQuery.getRawMany(), countQuery.getCount()]);

    return { data, count };
  },

  async listForUser(
    user: User,
    locale: Locale,
    page: number,
    limit: number,
    search?: string
  ): Promise<ResultsetWithCount<DatasetListItemDTO>> {
    logger.debug(`Listing datasets for user ${user.id}, language ${locale}, page ${page}, limit ${limit}`);
    const lang = locale.includes('en') ? Locale.EnglishGb : Locale.WelshGb;

    const groupIds = getUserGroupIdsForUser(user) || [];

    if (groupIds.length === 0) {
      return { data: [], count: 0 };
    }

    const query = listAllQuery(this.createQueryBuilder('d'), lang);
    query.where('d.userGroupId IN (:...groupIds)', { groupIds });

    if (search) {
      query.andWhere('r.title ILIKE :search', { search: `%${search}%` });
    }

    const offset = (page - 1) * limit;

    const countQuery = query.clone();
    const resultQuery = query.orderBy('r.updated_at', 'DESC').offset(offset).limit(limit);
    const [data, count] = await Promise.all([resultQuery.getRawMany(), countQuery.getCount()]);

    return { data, count };
  },

  async publish(revision: Revision): Promise<Dataset> {
    const dataset = await this.getById(revision.datasetId, { startRevision: true });

    if (!dataset.startRevision) {
      throw new Error(`Dataset ${dataset.id} does not have a start revision`);
    }

    dataset.draftRevision = null;
    dataset.publishedRevision = revision;
    dataset.firstPublishedAt = dataset.startRevision!.publishAt;

    return this.save(dataset);
  },

  async archive(datasetId: string): Promise<Dataset> {
    logger.info(`Archiving dataset ${datasetId}`);
    const dataset = await this.getById(datasetId);
    dataset.archivedAt = new Date();
    return await this.save(dataset);
  },

  async unarchive(datasetId: string): Promise<Dataset> {
    logger.info(`Unarchiving dataset ${datasetId}`);
    const dataset = await this.getById(datasetId);
    dataset.archivedAt = null;
    return await this.save(dataset);
  },

  async getDashboardStats(lang: Locale): Promise<DatasetStats> {
    logger.debug('Getting dashboard statistics for datasets');

    const coreViewName = `${CORE_VIEW_NAME}_mat_en`;

    const statusQuery = this.query(`
      WITH dataset_stats AS (
        SELECT
          d.id,
          CASE
            WHEN d.archived_at IS NOT NULL AND d.archived_at < NOW() THEN 'archived'
            WHEN pr.unpublished_at IS NOT NULL AND pr.unpublished_at < NOW() THEN 'offline'
            WHEN d.first_published_at IS NOT NULL AND d.first_published_at < NOW() THEN 'live'
            ELSE 'new'
          END as status,
          CASE
            WHEN d.first_published_at IS NOT NULL AND t.action = 'publish' AND t.status = 'requested' THEN 'update_pending_approval'
            WHEN t.action = 'publish' AND t.status = 'requested' THEN 'pending_approval'
            WHEN t.action = 'publish' AND t.status = 'rejected' THEN 'changes_requested'
            WHEN t.action = 'unpublish' AND t.status = 'requested' THEN 'unpublish_requested'
            WHEN t.action = 'archive' AND t.status = 'requested' THEN 'archive_requested'
            WHEN t.action = 'unarchive' AND t.status = 'requested' THEN 'unarchive_requested'
            WHEN pr.unpublished_at IS NOT NULL AND pr.unpublished_at < NOW() THEN 'unpublished'
            WHEN d.first_published_at IS NOT NULL AND d.first_published_at < NOW() AND r.approved_at IS NOT NULL AND r.publish_at < NOW() THEN 'published'
            WHEN d.first_published_at IS NOT NULL AND d.first_published_at < NOW() AND r.approved_at IS NOT NULL AND r.publish_at > NOW() THEN 'update_scheduled'
            WHEN d.first_published_at IS NOT NULL AND d.first_published_at > NOW() AND r.approved_at IS NOT NULL AND r.publish_at > NOW() THEN 'scheduled'
            WHEN d.first_published_at IS NOT NULL AND d.first_published_at < NOW() AND r.approved_at IS NULL THEN 'update_incomplete'
            WHEN d.first_published_at IS NULL AND r.approved_at IS NULL THEN 'incomplete'
            ELSE 'incomplete'
          END as publishing_status
        FROM dataset d
        INNER JOIN (
          SELECT DISTINCT ON (rev.dataset_id) rev.*
          FROM revision rev
          ORDER BY rev.dataset_id, rev.created_at DESC
        ) r ON r.dataset_id = d.id
        LEFT JOIN revision pr ON d.published_revision_id = pr.id
        LEFT JOIN task t ON d.id = t.dataset_id AND t.open = true
      )
      SELECT
        COUNT(*) as total,
        COUNT(*) FILTER (WHERE publishing_status = 'published') as published,
        COUNT(*) FILTER (WHERE status = 'archived') as archived,
        COUNT(*) FILTER (WHERE status = 'offline') as offline,
        COUNT(*) FILTER (WHERE publishing_status = 'incomplete' OR publishing_status = 'update_incomplete') as incomplete,
        COUNT(*) FILTER (WHERE publishing_status = 'pending_approval' OR publishing_status = 'update_pending_approval') as pending_approval,
        COUNT(*) FILTER (WHERE publishing_status = 'scheduled' OR publishing_status = 'update_scheduled') as scheduled,
        COUNT(*) FILTER (WHERE publishing_status = 'unpublish_requested' OR publishing_status = 'archive_requested' OR publishing_status = 'unarchive_requested') as action_requested
      FROM dataset_stats
    `);

    const largestQuery = this.query(
      `
      WITH largest_tables AS (
        SELECT oid::regclass::text AS objectname, reltuples AS row_count, pg_relation_size(oid) AS size_bytes
        FROM pg_class
        WHERE relkind IN ('m')
        AND oid::regclass::text LIKE $1
        AND pg_relation_size(oid) > 0
        ORDER  BY reltuples DESC
      )
      SELECT
        r.dataset_id AS dataset_id,
        rm.title AS title,
        MAX(lt.row_count) AS row_count,
        MAX(lt.size_bytes) AS size_bytes
      FROM revision r
      INNER JOIN largest_tables lt ON '"'||r.id||'".'||$2 = lt.objectname
      INNER JOIN revision_metadata rm ON rm.revision_id = r.id AND rm.language LIKE $3
      GROUP BY r.dataset_id, rm.title, lt.row_count, lt.size_bytes
      ORDER BY lt.row_count DESC
      LIMIT 10;
    `,
      [`%${coreViewName}`, coreViewName, `${lang}%`]
    );

    const longestQuery = this.query(
      `
        SELECT r.dataset_id AS dataset_id, rm.title AS title,
        CASE
          WHEN r.approved_at IS NULL THEN EXTRACT(EPOCH FROM (NOW()::timestamp - r.created_at::timestamp))::int
          ELSE EXTRACT(EPOCH FROM (r.approved_at::timestamp - r.created_at::timestamp))::int
        END AS interval,
        CASE
          WHEN r.approved_at IS NOT NULL AND r.publish_at < NOW() THEN 'published'
          WHEN r.approved_at IS NOT NULL AND r.publish_at > NOW() THEN 'scheduled'
          ELSE 'incomplete'
        END AS status
        FROM revision r
        INNER JOIN revision_metadata rm ON rm.revision_id = r.id AND rm.language LIKE $1
        ORDER BY interval DESC
        LIMIT 10
      `,
      [`${lang}%`]
    );

    const [status, largest, longest] = await Promise.all([statusQuery, largestQuery, longestQuery]);

    const summary = {
      incomplete: Number(status[0].incomplete),
      pending_approval: Number(status[0].pending_approval),
      scheduled: Number(status[0].scheduled),
      published: Number(status[0].published),
      action_requested: Number(status[0].action_requested),
      archived: Number(status[0].archived),
      offline: Number(status[0].offline),
      total: Number(status[0].total)
    };

    return { summary, largest, longest };
  }
});
