import { SourceAssignmentDTO } from '../dtos/source-assignment-dto';
import { FileImport } from '../entities/dataset/file-import';
import { SourceType } from '../enums/source-type';

export const validateSourceAssignment = async (fileImport: FileImport, sourceAssignment: SourceAssignmentDTO[]) => {
    const existingSourceIds = await fileImport.sources.map((source) => source.id);
    const counts = { unknown: 0, dataValues: 0, footnotes: 0 };

    sourceAssignment.forEach((source) => {
        if (!existingSourceIds.includes(source.sourceId)) {
            throw new Error(`errors.invalid_source_id: ${source.sourceId}`);
        }

        if (source.sourceType === SourceType.Unknown) counts.unknown++;
        if (source.sourceType === SourceType.DataValues) counts.dataValues++;
        if (source.sourceType === SourceType.FootNotes) counts.footnotes++;
    });

    if (counts.unknown > 0) {
        throw new Error('errors.sources.unknowns_found');
    }

    if (counts.dataValues > 1) {
        throw new Error('errors.sources.multiple_datavalues');
    }

    if (counts.footnotes > 1) {
        throw new Error('errors.sources.multiple_footnotes');
    }
};
