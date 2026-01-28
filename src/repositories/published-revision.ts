import { dataSource } from '../db/data-source';
import { Revision } from '../entities/dataset/revision';
import { And, FindOneOptions, FindOptionsRelations, IsNull, LessThan, Not } from 'typeorm';

export const PublishedRevisionRepository = dataSource.getRepository(Revision).extend({
  async getById(id: string, relations: FindOptionsRelations<Revision> = {}): Promise<Revision> {
    const now = new Date();

    const findOptions: FindOneOptions<Revision> = {
      where: {
        id,
        publishAt: And(Not(IsNull()), LessThan(now)),
        approvedAt: And(Not(IsNull()), LessThan(now)),
        unpublishedAt: IsNull()
      },
      relations
    };

    return this.findOneOrFail(findOptions);
  },

  async getLatestByDatasetId(
    datasetId: string,
    relations: FindOptionsRelations<Revision> = {}
  ): Promise<Revision | null> {
    const now = new Date();

    const findOptions: FindOneOptions<Revision> = {
      where: {
        datasetId,
        publishAt: And(Not(IsNull()), LessThan(now)),
        approvedAt: And(Not(IsNull()), LessThan(now)),
        unpublishedAt: IsNull()
      },
      order: {
        publishAt: 'DESC'
      },
      relations
    };

    return this.findOne(findOptions);
  }
});
