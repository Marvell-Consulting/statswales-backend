import { FactTable } from '../entities/dataset/fact-table';
import { Dimension } from '../entities/dataset/dimension';
import { LookupTable } from '../entities/dataset/lookup-table';
import { FactTableInfo } from '../entities/dataset/fact-table-info';
import { SupportedLanguagues } from '../enums/locale';
import { Measure } from '../entities/dataset/measure';

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
