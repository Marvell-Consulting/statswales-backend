import { appDataSource } from '../db/data-source';
import { Dimension } from '../entities/dataset/dimension';

export const DimensionRepository = appDataSource.getRepository(Dimension).extend({
  async getById(dimensionId: string): Promise<Dimension> {
    return appDataSource.getRepository(Dimension).findOneOrFail({
      where: {
        id: dimensionId
      },
      relations: {
        metadata: true,
        lookupTable: true
      }
    });
  }
});
