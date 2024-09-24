import { Source } from '../entities/source';

export class SourceDTO {
    id: string;
    import_id: string;
    revision_id: string;
    // Commented out as we don't have lookup tables yet
    // lookup_table_revision_id?: string;
    column_index: number;
    csv_field: string;
    action?: string;
    type?: string | undefined;

    static async fromSource(source: Source): Promise<SourceDTO> {
        const sourceDto = new SourceDTO();
        sourceDto.id = source.id;
        sourceDto.import_id = (await source.import).id;
        sourceDto.revision_id = (await source.revision).id;
        // sourceDto.lookup_table_revision_id = (await source.lookupTableRevision)?.id;
        sourceDto.column_index = source.columnIndex;
        sourceDto.csv_field = source.csvField;
        sourceDto.action = source.action;
        sourceDto.type = source.type;
        return sourceDto;
    }
}
