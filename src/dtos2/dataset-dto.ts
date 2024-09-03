import { Dataset } from '../entity2/dataset';
import { Dimension } from '../entity2/dimension';
import { DimensionInfo } from '../entity2/dimension_info';
import { Source } from '../entity2/source';
import { Import } from '../entity2/import';
import { Revision } from '../entity2/revision';
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
    dimensionInfo?: DimensionInfoDTO[];
    dataset_id?: string;

    static async fromDimension(dimension: Dimension): Promise<DimensionDTO> {
        const dimDto = new DimensionDTO();
        dimDto.id = dimension.id;
        dimDto.type = dimension.type;
        dimDto.start_revision_id = (await dimension.start_revision).id;
        dimDto.finish_revision_id = (await dimension.finish_revision)?.id || '';
        dimDto.validator = dimension.validator;
        dimDto.dimensionInfo = (await dimension.dimensionInfo).map((dimInfo: DimensionInfo) => {
            const infoDto = new DimensionInfoDTO();
            infoDto.language = dimInfo.language;
            infoDto.name = dimInfo.name;
            infoDto.description = dimInfo.description;
            infoDto.notes = dimInfo.notes;
            return infoDto;
        });
        dimDto.sources = await Promise.all(
            (await dimension.sources).map(async (source: Source) => {
                const sourceDto = new SourceDTO();
                sourceDto.id = source.id;
                sourceDto.import_id = (await source.import).id;
                sourceDto.revision_id = (await source.revision).id;
                sourceDto.csv_field = source.csv_field;
                sourceDto.action = source.action;
                return sourceDto;
            })
        );
        return dimDto;
    }
}

export class ImportDTO {
    id: string;
    revision_id: string;
    mime_type: string;
    filename: string;
    hash: string;
    uploaded_at: string;
    type: string;
    location: string;
    sources?: SourceDTO[];

    static async fromImport(importEntity: Import): Promise<ImportDTO> {
        const dto = new ImportDTO();
        dto.id = importEntity.id;
        const revision = await importEntity.revision;
        dto.revision_id = revision.id;
        dto.mime_type = importEntity.mime_type;
        dto.filename = importEntity.filename;
        dto.hash = importEntity.hash;
        dto.uploaded_at = importEntity.uploaded_at?.toISOString() || '';
        dto.type = importEntity.type;
        dto.location = importEntity.location;
        dto.sources = await Promise.all(
            (await importEntity.sources).map(async (source: Source) => {
                const sourceDto = new SourceDTO();
                sourceDto.id = source.id;
                sourceDto.import_id = (await source.import).id;
                sourceDto.revision_id = (await source.revision).id;
                sourceDto.csv_field = source.csv_field;
                sourceDto.action = source.action;
                return sourceDto;
            })
        );
        return dto;
    }
}

export class RevisionDTO {
    id: string;
    revision_index: number;
    creation_date: string;
    previous_revision_id?: string;
    online_cube_filename?: string;
    publish_date?: string;
    approval_date?: string;
    approved_by?: string;
    created_by: string;
    imports: ImportDTO[];
    dataset_id?: string;

    static async fromRevision(revision: Revision): Promise<RevisionDTO> {
        const revDto = new RevisionDTO();
        revDto.id = revision.id;
        revDto.revision_index = revision.revision_index;
        revDto.dataset_id = (await revision.dataset).id;
        revDto.creation_date = revision.creation_date.toISOString();
        revDto.previous_revision_id = (await revision.previous_revision)?.id || '';
        revDto.online_cube_filename = revision.online_cube_filename;
        revDto.publish_date = revision.publish_date?.toISOString() || '';
        revDto.approval_date = revision.approval_date?.toISOString() || '';
        revDto.approved_by = (await revision.approved_by)?.name || undefined;
        revDto.created_by = (await revision.created_by).name;
        revDto.imports = await Promise.all(
            (await revision.imports).map(async (imp: Import) => {
                const impDto = new ImportDTO();
                impDto.id = imp.id;
                impDto.revision_id = (await imp.revision).id;
                impDto.mime_type = imp.mime_type;
                impDto.filename = imp.filename;
                impDto.hash = imp.hash;
                impDto.uploaded_at = imp.uploaded_at.toISOString();
                impDto.type = imp.type;
                impDto.location = imp.location;
                return impDto;
            })
        );
        return revDto;
    }
}

export class DatasetDTO {
    id: string;
    creation_date: string;
    created_by: string;
    live?: string;
    archive?: string;
    dimensions?: DimensionDTO[];
    revisions?: RevisionDTO[];
    datasetInfo?: DatasetInfoDTO[];

    static async fromDatasetShallow(dataset: Dataset): Promise<DatasetDTO> {
        const dto = new DatasetDTO();
        dto.id = dataset.id;
        dto.creation_date = dataset.creation_date.toISOString();
        dto.created_by = (await dataset.created_by).name;
        dto.live = dataset.live?.toISOString() || '';
        dto.archive = dataset.archive?.toISOString() || '';
        dto.datasetInfo = (await dataset.datasetInfo).map((datasetInfo: DatasetInfo) => {
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

    static async fromDatasetComplete(dataset: Dataset): Promise<DatasetDTO> {
        const dto = new DatasetDTO();
        dto.id = dataset.id;
        dto.creation_date = dataset.creation_date.toISOString();
        dto.created_by = (await dataset.created_by).name;
        dto.live = dataset.live?.toISOString() || '';
        dto.archive = dataset.archive?.toISOString() || '';
        dto.datasetInfo = (await dataset.datasetInfo).map((datasetInfo: DatasetInfo) => {
            const infoDto = new DatasetInfoDTO();
            infoDto.language = datasetInfo.language;
            infoDto.title = datasetInfo.title;
            infoDto.description = datasetInfo.description;
            return infoDto;
        });
        dto.dimensions = await Promise.all(
            (await dataset.dimensions).map(async (dimension: Dimension) => {
                const dimDto = new DimensionDTO();
                dimDto.id = dimension.id;
                dimDto.type = dimension.type;
                dimDto.start_revision_id = (await dimension.start_revision).id;
                dimDto.finish_revision_id = (await dimension.finish_revision)?.id || undefined;
                dimDto.validator = dimension.validator;
                dimDto.dimensionInfo = (await dimension.dimensionInfo).map((dimInfo: DimensionInfo) => {
                    const infoDto = new DimensionInfoDTO();
                    infoDto.language = dimInfo.language;
                    infoDto.name = dimInfo.name;
                    infoDto.description = dimInfo.description;
                    infoDto.notes = dimInfo.notes;
                    return infoDto;
                });
                dimDto.sources = await Promise.all(
                    (await dimension.sources).map(async (source: Source) => {
                        const sourceDto = new SourceDTO();
                        sourceDto.id = source.id;
                        sourceDto.import_id = (await source.import).id;
                        sourceDto.revision_id = (await source.revision).id;
                        sourceDto.csv_field = source.csv_field;
                        sourceDto.action = source.action;
                        return sourceDto;
                    })
                );
                return dimDto;
            })
        );
        dto.revisions = await Promise.all(
            (await dataset.revisions).map(async (revision: Revision) => {
                const revDto = new RevisionDTO();
                revDto.id = revision.id;
                revDto.revision_index = revision.revision_index;
                revDto.dataset_id = (await revision.dataset).id;
                revDto.creation_date = revision.creation_date.toISOString();
                revDto.previous_revision_id = (await revision.previous_revision)?.id || undefined;
                revDto.online_cube_filename = revision.online_cube_filename;
                revDto.publish_date = revision.publish_date?.toISOString() || '';
                revDto.approval_date = revision.approval_date?.toISOString() || '';
                revDto.approved_by = (await revision.approved_by)?.name || undefined;
                revDto.created_by = (await revision.created_by)?.name;
                revDto.imports = await Promise.all(
                    (await revision.imports).map(async (imp: Import) => {
                        const impDto = new ImportDTO();
                        impDto.id = imp.id;
                        impDto.revision_id = (await imp.revision).id;
                        impDto.mime_type = imp.mime_type;
                        impDto.filename = imp.filename;
                        impDto.hash = imp.hash;
                        impDto.uploaded_at = imp.uploaded_at.toISOString();
                        impDto.type = imp.type;
                        impDto.location = imp.location;
                        impDto.sources = await Promise.all(
                            (await imp.sources).map(async (source: Source) => {
                                const sourceDto = new SourceDTO();
                                sourceDto.id = source.id;
                                sourceDto.import_id = (await source.import).id;
                                sourceDto.revision_id = (await source.revision).id;
                                sourceDto.csv_field = source.csv_field;
                                sourceDto.action = source.action;
                                return sourceDto;
                            })
                        );
                        return impDto;
                    })
                );
                return revDto;
            })
        );
        return dto;
    }

    static async fromDatasetWithRevisions(dataset: Dataset): Promise<DatasetDTO> {
        const dto = new DatasetDTO();
        dto.id = dataset.id;
        dto.creation_date = dataset.creation_date.toISOString();
        dto.created_by = (await dataset.created_by).name;
        dto.live = dataset.live?.toISOString() || '';
        dto.archive = dataset.archive?.toISOString() || '';
        dto.datasetInfo = (await dataset.datasetInfo).map((datasetInfo: DatasetInfo) => {
            const infoDto = new DatasetInfoDTO();
            infoDto.language = datasetInfo.language;
            infoDto.title = datasetInfo.title;
            infoDto.description = datasetInfo.description;
            return infoDto;
        });
        dto.dimensions = [];
        dto.revisions = await Promise.all(
            (await dataset.revisions).map(async (revision: Revision) => {
                const revDto = new RevisionDTO();
                revDto.id = revision.id;
                revDto.revision_index = revision.revision_index;
                revDto.dataset_id = (await revision.dataset).id;
                revDto.creation_date = revision.creation_date.toISOString();
                revDto.previous_revision_id = (await revision.previous_revision).id;
                revDto.online_cube_filename = revision.online_cube_filename;
                revDto.publish_date = revision.publish_date?.toISOString() || '';
                revDto.approval_date = revision.approval_date?.toISOString() || '';
                revDto.approved_by = (await revision.approved_by)?.name || '';
                revDto.created_by = (await revision.created_by)?.name || '';
                revDto.imports = [];
                return revDto;
            })
        );
        return dto;
    }

    static async fromDatasetWithRevisionsAndImports(dataset: Dataset): Promise<DatasetDTO> {
        const dto = new DatasetDTO();
        dto.id = dataset.id;
        dto.creation_date = dataset.creation_date.toISOString();
        dto.created_by = (await dataset.created_by).name;
        dto.live = dataset.live?.toISOString() || '';
        dto.archive = dataset.archive?.toISOString() || '';
        dto.datasetInfo = (await dataset.datasetInfo).map((datasetInfo: DatasetInfo) => {
            const infoDto = new DatasetInfoDTO();
            infoDto.language = datasetInfo.language;
            infoDto.title = datasetInfo.title;
            infoDto.description = datasetInfo.description;
            return infoDto;
        });
        dto.dimensions = [];
        dto.revisions = await Promise.all(
            (await dataset.revisions).map(async (revision: Revision) => {
                const revDto = new RevisionDTO();
                revDto.id = revision.id;
                revDto.revision_index = revision.revision_index;
                revDto.creation_date = revision.creation_date.toISOString();
                revDto.previous_revision_id = (await revision.previous_revision)?.id || undefined;
                revDto.online_cube_filename = revision.online_cube_filename;
                revDto.publish_date = revision.publish_date?.toISOString() || '';
                revDto.approval_date = revision.approval_date?.toISOString() || '';
                revDto.approved_by = (await revision.approved_by)?.name || undefined;
                revDto.created_by = (await revision.created_by)?.name;
                revDto.imports = await Promise.all(
                    (await revision.imports).map((imp: Import) => {
                        const impDto = new ImportDTO();
                        impDto.id = imp.id;
                        impDto.mime_type = imp.mime_type;
                        impDto.filename = imp.filename;
                        impDto.hash = imp.hash;
                        impDto.uploaded_at = imp.uploaded_at.toISOString();
                        impDto.type = imp.type;
                        impDto.location = imp.location;
                        return impDto;
                    })
                );
                return revDto;
            })
        );
        return dto;
    }

    static async fromDatasetWithShallowDimensionsAndRevisions(dataset: Dataset): Promise<DatasetDTO> {
        const dto = new DatasetDTO();
        dto.id = dataset.id;
        dto.creation_date = dataset.creation_date.toISOString();
        dto.created_by = (await dataset.created_by).name;
        dto.live = dataset.live?.toISOString() || '';
        dto.archive = dataset.archive?.toISOString() || '';
        dto.datasetInfo = (await dataset.datasetInfo).map((datasetInfo: DatasetInfo) => {
            const infoDto = new DatasetInfoDTO();
            infoDto.language = datasetInfo.language;
            infoDto.title = datasetInfo.title;
            infoDto.description = datasetInfo.description;
            return infoDto;
        });
        dto.dimensions = await Promise.all(
            (await dataset.dimensions).map(async (dimension: Dimension) => {
                const dimDto = new DimensionDTO();
                dimDto.id = dimension.id;
                dimDto.type = dimension.type;
                dimDto.start_revision_id = (await dimension.start_revision).id;
                dimDto.finish_revision_id = (await dimension.finish_revision)?.id || undefined;
                dimDto.validator = dimension.validator;
                dimDto.dimensionInfo = (await dimension.dimensionInfo).map((dimInfo: DimensionInfo) => {
                    const infoDto = new DimensionInfoDTO();
                    infoDto.language = dimInfo.language;
                    infoDto.name = dimInfo.name;
                    infoDto.description = dimInfo.description;
                    infoDto.notes = dimInfo.notes;
                    return infoDto;
                });
                dimDto.sources = []; // Sources are intentionally empty in this method as per original code
                return dimDto;
            })
        );
        dto.revisions = await Promise.all(
            (await dataset.revisions).map(async (revision: Revision) => {
                const revDto = new RevisionDTO();
                revDto.id = revision.id;
                revDto.revision_index = revision.revision_index;
                revDto.dataset_id = (await revision.dataset).id;
                revDto.creation_date = revision.creation_date.toISOString();
                revDto.previous_revision_id = (await revision.previous_revision)?.id || undefined;
                revDto.online_cube_filename = revision.online_cube_filename;
                revDto.publish_date = revision.publish_date?.toISOString() || '';
                revDto.approval_date = revision.approval_date?.toISOString() || '';
                revDto.approved_by = (await revision.approved_by)?.name || '';
                revDto.created_by = (await revision.created_by)?.name || '';
                revDto.imports = []; // Imports are intentionally empty in this method as per original code
                return revDto;
            })
        );
        return dto;
    }
}
