import { SourceAssignmentDTO } from '../dtos/source-assignment-dto';
import { Dataset } from '../entities/dataset/dataset';
import { Dimension } from '../entities/dataset/dimension';
import { DimensionInfo } from '../entities/dataset/dimension-info';
import { DimensionType } from '../enums/dimension-type';
import { FactTable } from '../entities/dataset/fact-table';
import { SourceType } from '../enums/source-type';
import { AVAILABLE_LANGUAGES } from '../middleware/translation';
import { logger } from '../utils/logger';
import { SourceAssignmentException } from '../exceptions/source-assignment.exception';
import { FactTableInfo } from '../entities/dataset/fact-table-info';
import { Measure } from '../entities/dataset/measure';

export interface ValidatedSourceAssignment {
    dataValues: SourceAssignmentDTO | null;
    noteCodes: SourceAssignmentDTO | null;
    measure: SourceAssignmentDTO | null;
    dimensions: SourceAssignmentDTO[];
    ignore: SourceAssignmentDTO[];
}

export const validateSourceAssignment = (
    fileImport: FactTable,
    sourceAssignment: SourceAssignmentDTO[]
): ValidatedSourceAssignment => {
    let dataValues: SourceAssignmentDTO | null = null;
    let noteCodes: SourceAssignmentDTO | null = null;
    let measure: SourceAssignmentDTO | null = null;
    const dimensions: SourceAssignmentDTO[] = [];
    const ignore: SourceAssignmentDTO[] = [];

    sourceAssignment.map((sourceInfo) => {
        if (!fileImport.factTableInfo?.find((info: FactTableInfo) => info.columnName === sourceInfo.columnName)) {
            throw new Error(`Source with id ${sourceInfo.columnName} not found`);
        }

        switch (sourceInfo.sourceType) {
            case SourceType.DataValues:
                if (dataValues) {
                    throw new SourceAssignmentException('errors.too_many_data_values');
                }
                dataValues = sourceInfo;
                break;
            case SourceType.Measure:
                if (measure) {
                    throw new SourceAssignmentException('errors.too_many_measure');
                }
                measure = sourceInfo;
                break;
            case SourceType.NoteCodes:
                if (noteCodes) {
                    throw new SourceAssignmentException('errors.too_many_footnotes');
                }
                noteCodes = sourceInfo;
                break;
            case SourceType.Dimension:
                dimensions.push(sourceInfo);
                break;
            case SourceType.Ignore:
                ignore.push(sourceInfo);
                break;
            default:
                throw new SourceAssignmentException(`errors.invalid_source_type`);
        }
    });

    return { dataValues, measure, noteCodes, dimensions, ignore };
};

async function createUpdateDimension(
    dataset: Dataset,
    factTable: FactTable,
    columnDescriptor: SourceAssignmentDTO
): Promise<void> {
    const columnInfo = await FactTableInfo.findOneByOrFail({columnName: columnDescriptor.columnName, id: factTable.id });
    const existingDimension = await Dimension.findOneBy({dataset: dataset, factTableColumn: columnDescriptor.columnName});

    if (existingDimension) {
        logger.debug(`No Dimension to create as fact table for column ${existingDimension.factTableColumn} is already attached to one`);
        return;
    }

    logger.debug("The existing dimension is either a footnotes dimension or we don't have one... So lets create one");
    columnInfo.columnType = SourceType.Dimension;
    await columnInfo.save();

    const dimension = new Dimension();
    dimension.type = DimensionType.Raw;
    dimension.dataset = dataset;
    dimension.factTableColumn = columnInfo.columnName;
    const savedDimension = await dimension.save();

    AVAILABLE_LANGUAGES.map(async (lang: string) => {
        const dimensionInfo = new DimensionInfo();
        dimensionInfo.id = savedDimension.id;
        dimensionInfo.dimension = savedDimension;
        dimensionInfo.language = lang;
        dimensionInfo.name = columnInfo.columnName;
        await dimensionInfo.save();
    });
}

async function cleanupDimensions(datasetId: string, factTableInfo: FactTableInfo[]): Promise<void> {
    const dataset = await Dataset.findOneOrFail({
        where: { id: datasetId },
        relations: ['dimensions']
    });

    const revisedDimensions = dataset.dimensions;

    for (const dimension of revisedDimensions) {
        if (!factTableInfo.find((factTableInfo) => factTableInfo.columnName === dimension.factTableColumn)) {
            await dimension.remove();
        }
    }
}

async function updateFactTableInfo(factTable: FactTable, updateColumnDto: SourceAssignmentDTO) {
    const info = factTable.factTableInfo.find((factTableInfo) => factTableInfo.columnName === updateColumnDto.columnName);
    if (!info) {
        throw new Error('No such column');
    }
    info.columnType = updateColumnDto.sourceType;
    await info.save();
}

async function createUpdateMeasure(dataset: Dataset, factTable: FactTable, columnAssignment: SourceAssignmentDTO): Promise<void> {
    const columnInfo = await FactTableInfo.findOneByOrFail({
        columnName: columnAssignment.columnName,
        id: factTable.id
    });
    const existingMeasure = await Measure.findOneBy({ dataset });

    if (existingMeasure && existingMeasure.factTableColumn === columnAssignment.columnName) {
        logger.debug(
            `No measure to create as fact table for column ${existingMeasure.factTableColumn} is already attached to one`
        );
        return;
    }

    columnInfo.columnType = SourceType.Measure;
    await columnInfo.save();

    if (existingMeasure && existingMeasure.factTableColumn !== columnAssignment.columnName) {
        existingMeasure.factTableColumn = columnAssignment.columnName;
        await existingMeasure.save();
        return;
    }

    const measure = new Measure();
    measure.factTableColumn = columnAssignment.columnName;
    measure.dataset = dataset;
    await measure.save();
    await dataset.save();
}

async function createUpdateNoteCodes(dataset: Dataset, factTable: FactTable, columnAssignment: SourceAssignmentDTO) {
    const columnInfo = await FactTableInfo.findOneByOrFail({
        columnName: columnAssignment.columnName,
        id: factTable.id
    });
    const existingDimension = await Dimension.findOneBy({ dataset, type: DimensionType.NoteCodes });

    if (existingDimension && existingDimension.factTableColumn === columnAssignment.columnName) {
        logger.debug(
            `No NotesCode Dimension to create as fact table for column ${existingDimension.factTableColumn} is already attached to one`
        );
        return;
    }

    columnInfo.columnType = SourceType.NoteCodes;
    await columnInfo.save();

    if (existingDimension && existingDimension.factTableColumn !== columnAssignment.columnName) {
        existingDimension.factTableColumn = columnAssignment.columnName;
        await existingDimension.save();
        return;
    }

    const dimension = new Dimension();
    dimension.type = DimensionType.NoteCodes;
    dimension.dataset = dataset;
    dimension.factTableColumn = columnInfo.columnName;
    dimension.joinColumn = 'NoteCode';
    const savedDimension = await dimension.save();

    AVAILABLE_LANGUAGES.map(async (lang: string) => {
        const dimensionInfo = new DimensionInfo();
        dimensionInfo.id = savedDimension.id;
        dimensionInfo.dimension = savedDimension;
        dimensionInfo.language = lang;
        dimensionInfo.name = columnInfo.columnName;
        await dimensionInfo.save();
    });
}

export const createDimensionsFromSourceAssignment = async (
    dataset: Dataset,
    factTable: FactTable,
    sourceAssignment: ValidatedSourceAssignment
): Promise<void> => {
    const { dataValues, measure, ignore, noteCodes, dimensions } = sourceAssignment;

    if (dataValues) {
        await updateFactTableInfo(factTable, dataValues);
    }

    if (noteCodes) {
        await createUpdateNoteCodes(dataset, factTable, noteCodes);
    }

    if (measure) {
        await createUpdateMeasure(dataset, factTable, measure);
    }

    await Promise.all(
        dimensions.map(async (dimensionCreationDTO: SourceAssignmentDTO) => {
            await createUpdateDimension(dataset, factTable, dimensionCreationDTO);
        })
    );

    await Promise.all(
        ignore.map(async (dimensionCreationDTO: SourceAssignmentDTO) => {
            await updateFactTableInfo(factTable, dimensionCreationDTO);
        })
    );

    await cleanupDimensions(dataset.id, factTable.factTableInfo);
};
