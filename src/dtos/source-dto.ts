import { Source } from '../entities/dataset/source';

export class SourceDTO {
    id: string;
    import_id: string;
    revision_id: string;
    dimension_id: string;
    // Commented out as we don't have lookup tables yet
    // lookup_table_revision_id?: string;
    column_index: number;
    csv_field: string;
    action?: string;
    type?: string | undefined;

    static fromSource(source: Source): SourceDTO {
        const sourceDto = new SourceDTO();
        sourceDto.id = source.id;
        sourceDto.import_id = source.importId;
        sourceDto.revision_id = source.revisionId;
        sourceDto.dimension_id = source.dimensionId;
        // sourceDto.lookup_table_revision_id = (source.lookupTableRevision)?.id;
        sourceDto.column_index = source.columnIndex;
        sourceDto.csv_field = source.csvField;
        sourceDto.action = source.action;
        sourceDto.type = source.type;

        return sourceDto;
    }
}
