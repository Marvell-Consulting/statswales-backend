import { Dimension as DimDto } from './dimension';
import { FullRevision, LiteRevision } from './revision';
import { Measure } from './measure';
import { DatasetRepository } from '../../repositories/dataset';
import { Revision } from '../../entities/dataset/revision';
import { DimensionRepository } from '../../repositories/dimension';
import { RevisionRepository, withMetadataProvidersAndTopic } from '../../repositories/revision';
import { PublisherDTO } from '../publisher-dto';
import { UserGroupRepository } from '../../repositories/user-group';
import { Locale } from '../../enums/locale';

export class DatasetDTO {
  id: string;
  published_revision?: FullRevision;
  data_description?: Measure;
  dimensions?: DimDto[];
  revisions: LiteRevision[];
  created_at: string;
  first_published_at?: string;
  archived_at?: string;
  publisher?: PublisherDTO;

  static async fromDatasetId(id: string, language: string): Promise<DatasetDTO> {
    const dataset = await DatasetRepository.getById(id, { measure: { measureTable: true } });
    const dimensions = await DimensionRepository.getByDatasetId(id);
    const revisions = await Revision.findBy({ datasetId: id });
    const publishedRevision = await RevisionRepository.getById(
      dataset.publishedRevisionId!,
      withMetadataProvidersAndTopic
    );
    const datasetDTO = new DatasetDTO();
    if (dataset.userGroupId) {
      const userGroup = await UserGroupRepository.getByIdWithOrganisation(dataset.userGroupId);
      datasetDTO.publisher = PublisherDTO.fromUserGroup(userGroup, language as Locale);
    }
    datasetDTO.revisions = revisions.map((revision) => {
      return {
        id: revision.id,
        revision_index: revision.revisionIndex,
        previous_revision_id: revision.previousRevisionId,
        updated_at: revision.updatedAt.toISOString(),
        publish_at: revision.publishAt?.toISOString(),
        coverage_start_date: revision.startDate?.toISOString(),
        coverage_end_date: revision.endDate?.toISOString()
      };
    });
    datasetDTO.dimensions = dimensions.map((dimension) => {
      const metaData = dimension.metadata.find((meta) => meta.language === language);
      return {
        id: dimension.id,
        factTableColumn: dimension.factTableColumn, // <-- Tells you which column in the fact table you're joining to
        metadata: metaData
          ? {
              language: metaData.language,
              name: metaData.name,
              description: metaData.description,
              notes: metaData.notes
            }
          : undefined
      };
    });
    const measureTable = dataset.measure.measureTable?.filter((row) => row.language === language);
    datasetDTO.data_description = {
      id: dataset.measure.id,
      fact_table_column: dataset.measure.factTableColumn,
      measure_table: measureTable
        ? measureTable.map((row) => {
            return {
              reference: row.reference,
              sort_order: row.sortOrder ? row.sortOrder : undefined,
              description: row.description,
              notes: row.notes ? row.notes : undefined,
              format: row.format,
              decimals: row.decimal ? row.decimal : undefined,
              measure_type: row.measureType ? row.measureType : undefined,
              hierarchy: row.hierarchy ? row.hierarchy : undefined
            };
          })
        : undefined
    };
    datasetDTO.id = dataset.id;
    datasetDTO.created_at = dataset.createdAt.toISOString();
    datasetDTO.first_published_at = dataset.firstPublishedAt?.toISOString();
    datasetDTO.archived_at = dataset.archivedAt?.toISOString();
    datasetDTO.published_revision = FullRevision.fromRevision(publishedRevision, language);
    return datasetDTO;
  }
}
