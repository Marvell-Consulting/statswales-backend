import { performance } from 'node:perf_hooks';

import {
  FindOneOptions,
  FindManyOptions,
  And,
  Not,
  IsNull,
  LessThan,
  FindOptionsRelations,
  In,
  Like,
  Raw
} from 'typeorm';
import { has, isObjectLike, omit, set } from 'lodash';

import { logger } from '../utils/logger';
import { dataSource } from '../db/data-source';
import { Dataset } from '../entities/dataset/dataset';
import { DatasetListItemDTO } from '../dtos/dataset-list-item-dto';
import { ResultsetWithCount } from '../interfaces/resultset-with-count';
import { Locale } from '../enums/locale';
import { Topic } from '../entities/dataset/topic';
import { Revision } from '../entities/dataset/revision';
import { SortByInterface } from '../interfaces/sort-by-interface';

export const withAll: FindOptionsRelations<Dataset> = {
  createdBy: true,
  factTable: true,
  dimensions: { metadata: true, lookupTable: true },
  measure: { measureTable: true, lookupTable: true },
  publishedRevision: {
    metadata: true,
    revisionProviders: { provider: true, providerSource: true },
    revisionTopics: { topic: true }
  },
  revisions: true
};

export const PublishedDatasetRepository = dataSource.getRepository(Dataset).extend({
  async getById(id: string, relations: FindOptionsRelations<Dataset> = {}): Promise<Dataset> {
    const start = performance.now();
    const now = new Date();
    const publishedRevRelations = relations.publishedRevision;

    const findOptions: FindOneOptions<Dataset> = {
      relations: omit(relations, 'publishedRevision'), // prevent direct use of publishedRevision relation
      where: {
        id,
        firstPublishedAt: And(Not(IsNull()), LessThan(now))
      }
    };

    if (has(relations, 'revisions')) {
      set(findOptions, 'where.revisions', { approvedAt: LessThan(now), publishAt: LessThan(now) });
      set(findOptions, 'order', { revisions: { publishAt: 'DESC' } });
    }

    const dataset = await this.findOneOrFail(findOptions);

    if (publishedRevRelations) {
      const relations = isObjectLike(publishedRevRelations)
        ? (publishedRevRelations as FindOptionsRelations<Revision>)
        : undefined;

      // publishedRevision must be manually loaded to ensure the publish_at has passed
      const publishedRevision = await dataSource.getRepository(Revision).findOne({
        where: { datasetId: id, approvedAt: LessThan(now), publishAt: LessThan(now) },
        order: { publishAt: 'DESC' },
        relations
      });
      dataset.publishedRevision = publishedRevision;
    }

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

  async listPublishedByLanguage(
    lang: Locale,
    page: number,
    limit: number
  ): Promise<ResultsetWithCount<DatasetListItemDTO>> {
    const qb = this.createQueryBuilder('d')
      .select([
        'd.id AS id',
        'r.title AS title',
        'd.first_published_at AS first_published_at',
        'r.publish_at AS last_updated_at',
        'd.archived_at AS archived_at'
      ])
      .innerJoin(
        (subQuery) => {
          // only join the latest published revision for each dataset
          return subQuery
            .select('DISTINCT ON (rev.dataset_id) rev.*, rm.title AS title')
            .from(Revision, 'rev')
            .innerJoin('rev.metadata', 'rm', 'rm.revision_id = rev.id AND rm.language LIKE :lang', { lang: `${lang}%` })
            .andWhere('rev.publish_at < NOW()')
            .andWhere('rev.approved_at < NOW()')
            .orderBy('rev.dataset_id')
            .addOrderBy('rev.publish_at', 'DESC');
        },
        'r',
        'r.dataset_id = d.id'
      )
      .andWhere('d.first_published_at IS NOT NULL')
      .andWhere('d.first_published_at < NOW()')
      .groupBy('d.id, r.id, r.title, d.first_published_at, r.publish_at, d.archived_at');

    const offset = (page - 1) * limit;
    const countQuery = qb.clone();
    const resultQuery = qb.orderBy('d.first_published_at', 'DESC').offset(offset).limit(limit);
    const [data, count] = await Promise.all([resultQuery.getRawMany(), countQuery.getCount()]);

    return { data, count };
  },

  async listPublishedTopics(lang: Locale, topicId?: string): Promise<Topic[]> {
    const latestPublishedRevisions = await dataSource
      .getRepository(Revision)
      .createQueryBuilder('r')
      .select('DISTINCT ON (r.dataset_id) r.id AS id')
      .where('r.publish_at < NOW()')
      .andWhere('r.approved_at < NOW()')
      .orderBy('r.dataset_id')
      .addOrderBy('r.publish_at', 'DESC')
      .getRawMany();

    const revisionIds = latestPublishedRevisions.map((revision) => revision.id);

    // if no topicId provided, fetch topics where path equals the id (i.e. root level topics)
    const path = topicId ? { path: Like(`${topicId}.%`) } : { path: Raw('"Topic"."id"::text') };

    const findOpts: FindManyOptions<Topic> = {
      where: {
        revisionTopics: { revisionId: In(revisionIds) },
        ...path
      },
      order: lang.includes(Locale.Welsh) ? { nameCY: 'ASC' } : { nameEN: 'ASC' }
    };

    return this.manager.getRepository(Topic).find(findOpts);
  },

  async listPublishedByTopic(
    topicId: string,
    lang: Locale,
    page: number,
    limit: number,
    sortBy?: SortByInterface[]
  ): Promise<ResultsetWithCount<DatasetListItemDTO>> {
    const qb = this.createQueryBuilder('d')
      .select([
        'd.id AS id',
        'r.title AS title',
        'd.first_published_at AS first_published_at',
        'r.publish_at AS last_updated_at',
        'd.archived_at AS archived_at'
      ])
      .innerJoin(
        (subQuery) => {
          // only include the latest published revision for each dataset, with their topics
          return subQuery
            .select(
              'DISTINCT ON (rev.dataset_id) rev.dataset_id, rev.id, rev.publish_at, rm.title AS title, ARRAY_AGG(rt.topic_id::TEXT) AS topic_ids'
            )
            .from(Revision, 'rev')
            .innerJoin('rev.metadata', 'rm', 'rm.revision_id = rev.id AND rm.language LIKE :lang', { lang: `${lang}%` })
            .innerJoin('rev.revisionTopics', 'rt')
            .andWhere('rev.publish_at < NOW()')
            .andWhere('rev.approved_at < NOW()')
            .groupBy('rev.dataset_id, rev.id, rev.publish_at, rm.title')
            .orderBy('rev.dataset_id')
            .addOrderBy('rev.publish_at', 'DESC');
        },
        'r',
        'r.dataset_id = d.id'
      )
      .andWhere('r.topic_ids @> ARRAY[:topicId]', { topicId })
      .andWhere('d.first_published_at IS NOT NULL')
      .andWhere('d.first_published_at < NOW()')
      .groupBy('d.id, r.id, r.title, d.first_published_at, r.publish_at, d.archived_at');

    const offset = (page - 1) * limit;
    const countQuery = qb.clone();
    const resultQuery = qb.offset(offset).limit(limit);

    if (!sortBy || sortBy.length === 0) {
      sortBy = [{ columnName: 'first_published_at', direction: 'DESC' }];
    }

    const sortOpts: Record<string, string> = {
      title: 'r.title',
      first_published_at: 'd.first_published_at',
      last_updated_at: 'r.publish_at'
    };

    sortBy.forEach((sort: SortByInterface) => {
      const column = sortOpts[sort.columnName];
      const direction = sort.direction === 'DESC' ? 'DESC' : 'ASC';
      resultQuery.addOrderBy(column, direction);
    });

    const [data, count] = await Promise.all([resultQuery.getRawMany(), countQuery.getCount()]);

    return { data, count };
  }
});
