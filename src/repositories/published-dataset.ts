import { FindOneOptions, And, Not, IsNull, LessThan, FindOptionsRelations } from 'typeorm';
import { has, set } from 'lodash';

import { dataSource } from '../db/data-source';
import { Dataset } from '../entities/dataset/dataset';
import { DatasetListItemDTO } from '../dtos/dataset-list-item-dto';
import { ResultsetWithCount } from '../interfaces/resultset-with-count';
import { Locale } from '../enums/locale';

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
    const now = new Date();

    const findOptions: FindOneOptions<Dataset> = {
      relations,
      where: {
        id,
        live: And(Not(IsNull()), LessThan(now)),
        revisions: {
          approvedAt: LessThan(now),
          publishAt: LessThan(now)
        }
      }
    };

    if (has(relations, 'revisions')) {
      set(findOptions, 'where.revisions', { approvedAt: LessThan(now), publishAt: LessThan(now) });
      set(findOptions, 'order', { revisions: { publishAt: 'DESC' } });
    }

    return this.findOneOrFail(findOptions);
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
  }
});
