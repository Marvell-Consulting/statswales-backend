import { dataSource } from '../db/data-source';
import { Dimension } from '../entities/dataset/dimension';

export const DimensionRepository = dataSource.getRepository(Dimension).extend({
  async getById(dimensionId: string): Promise<Dimension> {
    return dataSource.getRepository(Dimension).findOneOrFail({
      where: {
        id: dimensionId
      },
      relations: {
        metadata: true,
        lookupTable: true
      }
    });
  },

  async getByDatasetId(id: string): Promise<Dimension[]> {
    return dataSource.getRepository(Dimension).find({
      where: {
        datasetId: id
      },
      relations: {
        metadata: true
      }
    });
  }
});
