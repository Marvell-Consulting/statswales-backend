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
            .addOrderBy('rev.created_at', 'DESC')
            .take(1);
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
      .select('r.id AS id')
      .where('r.publish_at < NOW()')
      .andWhere('r.approved_at < NOW()')
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
            .innerJoin('rev.revisionTopics', 'rt')
            .where('rt.topicId = :topicId', { topicId })
            .andWhere('rev.publish_at < NOW()')
            .andWhere('rev.approved_at < NOW()')
            .orderBy('rev.dataset_id')
            .addOrderBy('rev.created_at', 'DESC')
            .take(1);
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
  }
});
