import { Dataset } from '../entity2/dataset';
import { Dimension } from '../entity2/dimension';
import { DimensionInfo } from '../entity2/dimension_info';
import { Source } from '../entity2/source';
import { Import } from '../entity2/import';
import { RevisionEntity } from '../entity2/revision';
import { DatasetInfo } from '../entity2/dataset_info';

export class DatasetInfoDTO {
    language?: string;
    title?: string;
    description?: string;
}

export class DimensionInfoDTO {
    language?: string;
    name: string;
    description?: string;
    notes?: string;
}

export class SourceDTO {
    id: string;
    import_id: string;
    revision_id: string;
    // Commented out as we don't have lookup tables yet
    // lookup_table_revision_id?: string;
    csv_field: string;
    action: string;
}

export class DimensionDTO {
    id: string;
    type: string;
    start_revision_id: string;
    finish_revision_id?: string;
    validator?: string;
    sources?: SourceDTO[];
    dimensionInfos?: DimensionInfoDTO[];
    dataset_id?: string;

    static fromDimension(dimension: Dimension): DimensionDTO {
        const dto = new DimensionDTO();
        dto.id = dimension.id;
        dto.type = dimension.type;
        dto.start_revision_id = dimension.start_revision.id;
        dto.finish_revision_id = dimension.finish_revision ? dimension.finish_revision.id : undefined;
        dto.validator = dimension.validator;
        dto.dimensionInfos = dimension.dimensionInfos.map((dimensionInfo: DimensionInfo) => {
            const infoDto = new DimensionInfoDTO();
            infoDto.language = dimensionInfo.language;
            infoDto.name = dimensionInfo.name;
            infoDto.description = dimensionInfo.description;
            infoDto.notes = dimensionInfo.notes;
            return infoDto;
        });
        dto.sources = dimension.sources.map((source: Source) => {
            const sourceDto = new SourceDTO();
            sourceDto.id = source.id;
            sourceDto.import_id = source.import.id;
            sourceDto.revision_id = source.revision.id;
            sourceDto.csv_field = source.csv_field;
            sourceDto.action = source.action;
            return sourceDto;
        });
        dto.dataset_id = dimension.dataset.id;
        return dto;
    }
}

export class ImportDTO {
    id: string;
    revision_id: string;
    mime_type: string;
    filename: string;
    hash: string;
    uploaded_at: Date;
    type: string;
    location: string;

    static fromImport(importEntity: Import): ImportDTO {
        const dto = new ImportDTO();
        dto.id = importEntity.id;
        dto.revision_id = importEntity.revision.id;
        dto.mime_type = importEntity.mime_type;
        dto.filename = importEntity.filename;
        dto.hash = importEntity.hash;
        dto.uploaded_at = importEntity.uploaded_at;
        dto.type = importEntity.type;
        dto.location = importEntity.location;
        return dto;
    }
}

export class RevisionDTO {
    id: string;
    revision_index: number;
    creation_date: Date;
    previous_revision_id?: string;
    online_cube_filename?: string;
    publish_date?: Date;
    approval_date?: Date;
    approved_by?: string;
    created_by: string;
    imports: ImportDTO[];
    dataset_id?: string;

    static fromRevision(revision: RevisionEntity): RevisionDTO {
        const dto = new RevisionDTO();
        dto.id = revision.id;
        dto.revision_index = revision.revision_index;
        dto.dataset_id = revision.dataset.id;
        dto.creation_date = revision.creation_date;
        dto.previous_revision_id = revision.previous_revision ? revision.previous_revision.id : undefined;
        dto.online_cube_filename = revision.online_cube_filename;
        dto.publish_date = revision.publish_date;
        dto.approval_date = revision.approval_date;
        dto.approved_by = revision.approved_by ? revision.approved_by.name : undefined;
        dto.created_by = revision.created_by.name;
        dto.imports = revision.imports.map((importEntity: Import) => {
            return ImportDTO.fromImport(importEntity);
        });
        return dto;
    }
}

export class DatasetDTO {
    id: string;
    creation_date: Date;
    created_by: string;
    live?: Date;
    archive?: Date;
    dimensions?: DimensionDTO[];
    revisions?: RevisionDTO[];
    datasetInfos?: DatasetInfoDTO[];

    static fromDatasetComplete(dataset: Dataset): DatasetDTO {
        const dto = new DatasetDTO();
        dto.id = dataset.id;
        dto.creation_date = dataset.creation_date;
        dto.created_by = dataset.created_by.name;
        dto.live = dataset.live;
        dto.archive = dataset.archive;
        dto.datasetInfos = dataset.datasetInfos.map((datasetInfo: DatasetInfo) => {
            const infoDto = new DatasetInfoDTO();
            infoDto.language = datasetInfo.language;
            infoDto.title = datasetInfo.title;
            infoDto.description = datasetInfo.description;
            return infoDto;
        });
        dto.dimensions = dataset.dimensions.map((dimension: Dimension) => {
            const dimDto = new DimensionDTO();
            dimDto.id = dimension.id;
            dimDto.type = dimension.type;
            dimDto.start_revision_id = dimension.start_revision.id;
            dimDto.finish_revision_id = dimension.finish_revision.id;
            dimDto.validator = dimension.validator;
            dimDto.dimensionInfos = dimension.dimensionInfos.map((dimInfo: DimensionInfo) => {
                const infoDto = new DimensionInfoDTO();
                infoDto.language = dimInfo.language;
                infoDto.name = dimInfo.name;
                infoDto.description = dimInfo.description;
                infoDto.notes = dimInfo.notes;
                return infoDto;
            });
            dimDto.sources = dimension.sources.map((source: Source) => {
                const sourceDto = new SourceDTO();
                sourceDto.id = source.id;
                sourceDto.import_id = source.import.id;
                sourceDto.revision_id = source.revision.id;
                sourceDto.csv_field = source.csv_field;
                sourceDto.action = source.action;
                return sourceDto;
            });
            return dimDto;
        });
        dto.revisions = dataset.revisions.map((revision: RevisionEntity) => {
            const revDto = new RevisionDTO();
            revDto.id = revision.id;
            revDto.revision_index = revision.revision_index;
            revDto.dataset_id = revision.dataset.id;
            revDto.creation_date = revision.creation_date;
            revDto.previous_revision_id = revision.previous_revision.id;
            revDto.online_cube_filename = revision.online_cube_filename;
            revDto.publish_date = revision.publish_date;
            revDto.approval_date = revision.approval_date;
            revDto.approved_by = revision.approved_by.name;
            revDto.created_by = revision.created_by.name;
            revDto.imports = revision.imports.map((imp: Import) => {
                const impDto = new ImportDTO();
                impDto.id = imp.id;
                impDto.revision_id = imp.revision.id;
                impDto.mime_type = imp.mime_type;
                impDto.filename = imp.filename;
                impDto.hash = imp.hash;
                impDto.uploaded_at = imp.uploaded_at;
                impDto.type = imp.type;
                impDto.location = imp.location;
                return impDto;
            });
            return revDto;
        });
        return dto;
    }

    static fromDatasetWithDimensions(dataset: Dataset): DatasetDTO {
        const dto = new DatasetDTO();
        dto.id = dataset.id;
        dto.creation_date = dataset.creation_date;
        dto.created_by = dataset.created_by.name;
        dto.live = dataset.live;
        dto.archive = dataset.archive;
        dto.datasetInfos = dataset.datasetInfos.map((datasetInfo: DatasetInfo) => {
            const infoDto = new DatasetInfoDTO();
            infoDto.language = datasetInfo.language;
            infoDto.title = datasetInfo.title;
            infoDto.description = datasetInfo.description;
            return infoDto;
        });
        dto.dimensions = dataset.dimensions.map((dimension: Dimension) => {
            const dimDto = new DimensionDTO();
            dimDto.id = dimension.id;
            dimDto.type = dimension.type;
            dimDto.start_revision_id = dimension.start_revision.id;
            dimDto.finish_revision_id = dimension.finish_revision.id;
            dimDto.validator = dimension.validator;
            dimDto.sources = dimension.sources.map((source: Source) => {
                const sourceDto = new SourceDTO();
                sourceDto.id = source.id;
                sourceDto.import_id = source.import.id;
                sourceDto.revision_id = source.revision.id;
                sourceDto.csv_field = source.csv_field;
                sourceDto.action = source.action;
                return sourceDto;
            });
            return dimDto;
        });
        dto.revisions = [];
        return dto;
    }

    static fromDatasetWithRevisions(dataset: Dataset): DatasetDTO {
        const dto = new DatasetDTO();
        dto.id = dataset.id;
        dto.creation_date = dataset.creation_date;
        dto.created_by = dataset.created_by.name;
        dto.live = dataset.live;
        dto.archive = dataset.archive;
        dto.datasetInfos = dataset.datasetInfos.map((datasetInfo: DatasetInfo) => {
            const infoDto = new DatasetInfoDTO();
            infoDto.language = datasetInfo.language;
            infoDto.title = datasetInfo.title;
            infoDto.description = datasetInfo.description;
            return infoDto;
        });
        dto.dimensions = [];
        dto.revisions = dataset.revisions.map((revision: RevisionEntity) => {
            const revDto = new RevisionDTO();
            revDto.id = revision.id;
            revDto.revision_index = revision.revision_index;
            revDto.dataset_id = revision.dataset.id;
            revDto.creation_date = revision.creation_date;
            revDto.previous_revision_id = revision.previous_revision.id;
            revDto.online_cube_filename = revision.online_cube_filename;
            revDto.publish_date = revision.publish_date;
            revDto.approval_date = revision.approval_date;
            revDto.approved_by = revision.approved_by.name;
            revDto.created_by = revision.created_by.name;
            revDto.imports = [];
            return revDto;
        });
        return dto;
    }

    static fromDatasetWithShallowDimensionsAndRevisions(dataset: Dataset): DatasetDTO {
        const dto = new DatasetDTO();
        dto.id = dataset.id;
        dto.creation_date = dataset.creation_date;
        dto.created_by = dataset.created_by.name;
        dto.live = dataset.live;
        dto.archive = dataset.archive;
        dto.datasetInfos = dataset.datasetInfos.map((datasetInfo: DatasetInfo) => {
            const infoDto = new DatasetInfoDTO();
            infoDto.language = datasetInfo.language;
            infoDto.title = datasetInfo.title;
            infoDto.description = datasetInfo.description;
            return infoDto;
        });
        dto.dimensions = dataset.dimensions.map((dimension: Dimension) => {
            const dimDto = new DimensionDTO();
            dimDto.id = dimension.id;
            dimDto.type = dimension.type;
            dimDto.start_revision_id = dimension.start_revision.id;
            dimDto.finish_revision_id = dimension.finish_revision.id;
            dimDto.validator = dimension.validator;
            dimDto.dimensionInfos = dimension.dimensionInfos.map((dimInfo: DimensionInfo) => {
                const infoDto = new DimensionInfoDTO();
                infoDto.language = dimInfo.language;
                infoDto.name = dimInfo.name;
                infoDto.description = dimInfo.description;
                infoDto.notes = dimInfo.notes;
                return infoDto;
            });
            dimDto.sources = [];
            return dimDto;
        });
        dto.revisions = dataset.revisions.map((revision: RevisionEntity) => {
            const revDto = new RevisionDTO();
            revDto.id = revision.id;
            revDto.revision_index = revision.revision_index;
            revDto.dataset_id = revision.dataset.id;
            revDto.creation_date = revision.creation_date;
            revDto.previous_revision_id = revision.previous_revision.id;
            revDto.online_cube_filename = revision.online_cube_filename;
            revDto.publish_date = revision.publish_date;
            revDto.approval_date = revision.approval_date;
            revDto.approved_by = revision.approved_by.name;
            revDto.created_by = revision.created_by.name;
            revDto.imports = [];
            return revDto;
        });
        return dto;
    }

    static fromDatasetWithImports(dataset: Dataset): DatasetDTO {
        const dto = new DatasetDTO();
        dto.id = dataset.id;
        dto.creation_date = dataset.creation_date;
        dto.created_by = dataset.created_by.name;
        dto.live = dataset.live;
        dto.archive = dataset.archive;
        dto.datasetInfos = dataset.datasetInfos.map((datasetInfo: DatasetInfo) => {
            const infoDto = new DatasetInfoDTO();
            infoDto.language = datasetInfo.language;
            infoDto.title = datasetInfo.title;
            infoDto.description = datasetInfo.description;
            return infoDto;
        });
        dto.dimensions = [];
        dto.revisions = dataset.revisions.map((revision: RevisionEntity) => {
            const revDto = new RevisionDTO();
            revDto.id = revision.id;
            revDto.revision_index = revision.revision_index;
            revDto.dataset_id = revision.dataset.id;
            revDto.creation_date = revision.creation_date;
            revDto.previous_revision_id = revision.previous_revision.id;
            revDto.online_cube_filename = revision.online_cube_filename;
            revDto.publish_date = revision.publish_date;
            revDto.approval_date = revision.approval_date;
            revDto.approved_by = revision.approved_by.name;
            revDto.created_by = revision.created_by.name;
            revDto.imports = revision.imports.map((imp: Import) => {
                const impDto = new ImportDTO();
                impDto.id = imp.id;
                impDto.revision_id = imp.revision.id;
                impDto.mime_type = imp.mime_type;
                impDto.filename = imp.filename;
                impDto.hash = imp.hash;
                impDto.uploaded_at = imp.uploaded_at;
                impDto.type = imp.type;
                impDto.location = imp.location;
                return impDto;
            });
            return revDto;
        });
        return dto;
    }

    // Returns a very shallow DTO with only the dataset info
    static fromDatasetShallow(dataset: Dataset): DatasetDTO {
        const dto = new DatasetDTO();
        dto.id = dataset.id;
        dto.creation_date = dataset.creation_date;
        dto.created_by = dataset.created_by.name;
        dto.live = dataset.live;
        dto.archive = dataset.archive;
        dto.datasetInfos = dataset.datasetInfos.map((datasetInfo: DatasetInfo) => {
            const infoDto = new DatasetInfoDTO();
            infoDto.language = datasetInfo.language;
            infoDto.title = datasetInfo.title;
            infoDto.description = datasetInfo.description;
            return infoDto;
        });
        dto.dimensions = [];
        dto.revisions = [];
        return dto;
    }
}
