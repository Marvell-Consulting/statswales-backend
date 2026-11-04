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
  },

  async getColumnsWithLookupsUploadedSince(datasetId: string, since: Date): Promise<string[]> {
    const rows: { fact_table_column: string }[] = await publisherDataSource.query(
      `SELECT d.fact_table_column FROM dimension d
         JOIN lookup_table lt ON lt.id = d.lookup_table_id
        WHERE d.dataset_id = $1 AND lt.uploaded_at > $2
       UNION
       SELECT m.fact_table_column FROM measure m
         JOIN lookup_table lt ON lt.id = m.lookup_table_id
        WHERE m.dataset_id = $1 AND lt.uploaded_at > $2`,
      [datasetId, since]
    );
    return rows.map((row) => row.fact_table_column);
  }
});
