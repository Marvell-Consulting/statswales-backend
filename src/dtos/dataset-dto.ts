import { Dataset } from '../entities/dataset/dataset';
import { Dimension } from '../entities/dataset/dimension';
import { Revision } from '../entities/dataset/revision';

import { DimensionDTO } from './dimension-dto';
import { RevisionDTO } from './revision-dto';
import { MeasureDTO } from './measure-dto';
import { FactTableColumnDto } from './fact-table-column-dto';
import { TaskDTO } from './task-dto';
import { Task } from '../entities/task/task';
import { PublisherDTO } from './publisher-dto';

export class DatasetDTO {
  id: string;
  created_at: string;
  created_by_id: string;
  first_published_at?: string | null;
  archived_at?: string;
  fact_table?: FactTableColumnDto[];
  dimensions?: DimensionDTO[];
  revisions: RevisionDTO[];
  start_revision?: RevisionDTO;
  start_revision_id?: string;
  end_revision?: RevisionDTO;
  end_revision_id?: string;
  draft_revision?: RevisionDTO;
  draft_revision_id?: string;
  published_revision?: RevisionDTO;
  published_revision_id?: string;
  measure?: MeasureDTO;
  start_date?: Date | null;
  end_date?: Date | null;
  user_group_id?: string;
  tasks?: TaskDTO[];
  publisher?: PublisherDTO;

  static fromDataset(dataset: Dataset): DatasetDTO {
    const dto = new DatasetDTO();
    dto.id = dataset.id;
    dto.created_at = dataset.createdAt.toISOString();
    dto.created_by_id = dataset.createdById;

    dto.first_published_at = dataset.firstPublishedAt?.toISOString();
    dto.archived_at = dataset.archivedAt?.toISOString();

    dto.dimensions = dataset.dimensions?.map((dimension: Dimension) => DimensionDTO.fromDimension(dimension));
    dto.revisions = dataset.revisions?.map((revision: Revision) => RevisionDTO.fromRevision(revision));

    dto.draft_revision_id = dataset.draftRevisionId;
    dto.start_revision_id = dataset.startRevisionId;
    dto.end_revision_id = dataset.endRevisionId;
    dto.published_revision_id = dataset.publishedRevisionId;

    dto.draft_revision = dataset.draftRevision ? RevisionDTO.fromRevision(dataset.draftRevision) : undefined;
    dto.start_revision = dataset.startRevision ? RevisionDTO.fromRevision(dataset.startRevision) : undefined;
    dto.end_revision = dataset.endRevision ? RevisionDTO.fromRevision(dataset.endRevision) : undefined;
    dto.published_revision = dataset.publishedRevision
      ? RevisionDTO.fromRevision(dataset.publishedRevision)
      : undefined;

    dto.measure = dataset.measure ? MeasureDTO.fromMeasure(dataset.measure) : undefined;

    if (dataset.factTable) {
      dto.fact_table = dataset.factTable.map((column) => FactTableColumnDto.fromFactTableColumn(column));
    }

    dto.start_date = dataset.startDate;
    dto.end_date = dataset.endDate;

    dto.user_group_id = dataset.userGroupId;

    dto.tasks = dataset.tasks?.map((task: Task) => TaskDTO.fromTask(task)) || [];

    return dto;
  }
}
