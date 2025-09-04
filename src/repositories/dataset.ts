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
import { PeriodCovered } from '../interfaces/period-covered';
import { User } from '../entities/user/user';
import { getUserGroupIdsForUser } from '../utils/get-permissions-for-user';

export const withStandardPreview: FindOptionsRelations<Dataset> = {
  createdBy: true,
  dimensions: { metadata: true },
  measure: { metadata: true },
  revisions: true,
  tasks: true
};

export const withDeveloperPreview: FindOptionsRelations<Dataset> = {
  createdBy: true,
  factTable: true,
  dimensions: { metadata: true },
  measure: { metadata: true, measureTable: true, lookupTable: true },
  revisions: { metadata: true }
};

export const withLatestRevision: FindOptionsRelations<Dataset> = {
  endRevision: { metadata: true }
};

export const withFactTable: FindOptionsRelations<Dataset> = {
  factTable: true
};

export const withDraftAndMetadata: FindOptionsRelations<Dataset> = {
  draftRevision: { metadata: true }
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

export const withDraftForCube: FindOptionsRelations<Dataset> = {
  factTable: true,
  draftRevision: { dataTable: { dataTableDescriptions: true } },
  dimensions: { metadata: true, lookupTable: true },
  measure: { metadata: true, measureTable: true, lookupTable: true },
  revisions: { dataTable: { dataTableDescriptions: true } }
};

const listAllQuery = (qb: QueryBuilder<Dataset>, lang: Locale): SelectQueryBuilder<Dataset> => {
  return qb
    .select(['d.id AS id', 'r.title AS title', 'r.title_alt AS title_alt', 'r.updated_at AS last_updated'])
    .addSelect(`ugm.name AS group_name`)
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
          WHEN d.live IS NOT NULL AND t.action = 'publish' AND t.status = 'requested' THEN 'update_pending_approval'
          WHEN t.action = 'publish' AND t.status = 'requested' THEN 'pending_approval'
          WHEN t.action = 'publish' AND t.status = 'rejected' THEN 'changes_requested'
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
    .innerJoin('d.userGroup', 'ug')
    .innerJoin('ug.metadata', 'ugm', 'ugm.language = :lang', { lang })
    .leftJoin('d.tasks', 't', 't.open = true')
    .groupBy('d.id, r.title, ugm.name, r.title_alt, r.updated_at, r.approved_at, r.publish_at, t.action, t.status');
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
  }
});
