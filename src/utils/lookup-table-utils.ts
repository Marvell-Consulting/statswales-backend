import { FactTable } from '../entities/dataset/fact-table';
import { Dimension } from '../entities/dataset/dimension';
import { LookupTable } from '../entities/dataset/lookup-table';
import { FactTableInfo } from '../entities/dataset/fact-table-info';
import { SupportedLanguagues } from '../enums/locale';
import { Measure } from '../entities/dataset/measure';
import { MeasureLookupPatchDTO } from '../dtos/measure-lookup-patch-dto';
import { LookupTablePatchDTO } from '../dtos/lookup-patch-dto';

import { logger } from './logger';

export function convertFactTableToLookupTable(factTable: FactTable, dimension?: Dimension, measure?: Measure) {
    const lookupTable = new LookupTable();
    lookupTable.id = factTable.id;
    lookupTable.fileType = factTable.fileType;
    lookupTable.filename = factTable.filename;
    lookupTable.mimeType = factTable.mimeType;
    lookupTable.hash = factTable.hash;
    lookupTable.delimiter = factTable.delimiter;
    lookupTable.linebreak = factTable.linebreak;
    lookupTable.quote = factTable.quote;
    if (dimension) lookupTable.dimension = dimension;
    if (measure) lookupTable.measure = measure;
    return lookupTable;
}

export function columnIdentification(info: FactTableInfo) {
    let lang = 'zz';
    for (const supLang of Object.values(SupportedLanguagues)) {
        if (info.columnName.toLowerCase().endsWith(supLang.code.toLowerCase())) {
            lang = supLang.code;
            break;
        } else if (info.columnName.toLowerCase().endsWith(supLang.name.toLowerCase())) {
            lang = supLang.code;
            break;
        }
    }
    return {
        name: info.columnName,
        lang
    };
}

// Look for the join column.  If there's a table matcher we always use this
// If the user has called the lookup table column the same as the fact table column use this
// If they've used the exact name in the guidance e.g. ref_code, reference_code, refcode use this
// Finally we do fuzzy matching where we exclude everything that isn't a protected name and see what we have left
export const lookForJoinColumn = (
    protoLookupTable: FactTable,
    factTableColumn: string,
    tableMatcher?: MeasureLookupPatchDTO | LookupTablePatchDTO
): string => {
    const refCol = protoLookupTable.factTableInfo.find((col) => col.columnName.toLowerCase().startsWith('ref'));
    if (tableMatcher?.join_column) {
        return tableMatcher.join_column;
    } else if (protoLookupTable.factTableInfo.find((col) => col.columnName === factTableColumn)) {
        return factTableColumn;
    } else if (refCol) {
        return refCol.columnName;
    } else {
        const possibleJoinColumns = protoLookupTable.factTableInfo.filter((info) => {
            if (info.columnName.toLowerCase().indexOf('decimal') >= 0) return false;
            if (info.columnName.toLowerCase().indexOf('hierarchy') >= 0) return false;
            if (info.columnName.toLowerCase().indexOf('format') >= 0) return false;
            if (info.columnName.toLowerCase().indexOf('description') >= 0) return false;
            if (info.columnName.toLowerCase().indexOf('sort') >= 0) return false;
            if (info.columnName.toLowerCase().indexOf('note') >= 0) return false;
            if (info.columnName.toLowerCase().indexOf('type') >= 0) return false;
            if (info.columnName.toLowerCase().indexOf('lang') >= 0) return false;
            logger.debug(`Looks like column ${info.columnName.toLowerCase()} is a join column`);
            return true;
        });
        if (possibleJoinColumns.length > 1) {
            throw new Error(
                `There are to many possible join columns.  Ask user for more information... Join columns present: ${possibleJoinColumns.join(', ')}`
            );
        }
        if (possibleJoinColumns.length === 0) {
            throw new Error('Could not find a column to join against the fact table.');
        }
        logger.debug(`Found the following join column ${JSON.stringify(possibleJoinColumns)}`);
        return possibleJoinColumns[0].columnName;
    }
};
