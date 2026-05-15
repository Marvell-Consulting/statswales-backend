import { publisherDataSource } from '../db/publisher-source';
import { Dimension } from '../entities/dataset/dimension';

export const DimensionRepository = publisherDataSource.getRepository(Dimension).extend({
  async getById(dimensionId: string): Promise<Dimension> {
    return publisherDataSource.getRepository(Dimension).findOneOrFail({
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
    return publisherDataSource.getRepository(Dimension).find({
      where: {
        datasetId: id
      },
      relations: {
        metadata: true
      }
    });
  }
});
