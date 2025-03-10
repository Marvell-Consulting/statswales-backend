import { Dataset } from '../entities/dataset/dataset';
import { Dimension } from '../entities/dataset/dimension';
import { Revision } from '../entities/dataset/revision';
import { SUPPORTED_LOCALES } from '../middleware/translation';

import { DimensionDTO } from './dimension-dto';
import { RevisionDTO } from './revision-dto';
import { MeasureDTO } from './measure-dto';
import { TeamDTO } from './team-dto';
import { FactTableColumnDto } from './fact-table-column-dto';

export class DatasetDTO {
  id: string;
  created_at: string;
  created_by_id: string;
  live?: string | null;
  archive?: string;
  fact_table?: FactTableColumnDto[];
  dimensions?: DimensionDTO[];
  revisions: RevisionDTO[];
  draft_revision?: RevisionDTO;
  measure?: MeasureDTO;
  team_id?: string;
  team?: TeamDTO[];
  start_date?: Date | null;
  end_date?: Date | null;

  static fromDataset(dataset: Dataset): DatasetDTO {
    const dto = new DatasetDTO();
    dto.id = dataset.id;
    dto.created_at = dataset.createdAt.toISOString();
    dto.created_by_id = dataset.createdById;

    dto.live = dataset.live?.toISOString();
    dto.archive = dataset.archive?.toISOString();

    dto.dimensions = dataset.dimensions?.map((dimension: Dimension) => DimensionDTO.fromDimension(dimension));
    dto.revisions = dataset.revisions?.map((revision: Revision) => RevisionDTO.fromRevision(revision));
    dto.draft_revision = dataset.draftRevision ? RevisionDTO.fromRevision(dataset.draftRevision) : undefined;

    dto.measure = dataset.measure ? MeasureDTO.fromMeasure(dataset.measure) : undefined;
    dto.team_id = dataset.teamId; // keep this because it means we don't need always hydrate the team relation

    if (dataset.team) {
      dto.team = SUPPORTED_LOCALES.map((locale) => {
        return TeamDTO.fromTeam(dataset.team, locale);
      });
    }

    if (dataset.factTable) {
      dto.fact_table = dataset.factTable.map((column) => FactTableColumnDto.fromFactTableColumn(column));
    }

    dto.start_date = dataset.startDate;
    dto.end_date = dataset.endDate;

    return dto;
  }
}
