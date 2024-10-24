import { SourceAssignmentDTO } from '../dtos/source-assignment-dto';
import { Dataset } from '../entities/dataset/dataset';
import { Dimension } from '../entities/dataset/dimension';
import { DimensionInfo } from '../entities/dataset/dimension-info';
import { DimensionType } from '../enums/dimension-type';
import { Revision } from '../entities/dataset/revision';
import { Source } from '../entities/dataset/source';
import { FileImport } from '../entities/dataset/file-import';
import { SourceType } from '../enums/source-type';
import { AVAILABLE_LANGUAGES, i18next } from '../middleware/translation';
import { SourceAction } from '../enums/source-action';
import { logger } from '../utils/logger';

export interface ValidatedDimensionCreationRequest {
    datavalues: SourceAssignmentDTO | null;
    footnotes: SourceAssignmentDTO | null;
    dimensions: SourceAssignmentDTO[];
    ignore: SourceAssignmentDTO[];
}

export const validateDimensionCreationRequest = (
    fileImport: FileImport,
    sourceAssignment: SourceAssignmentDTO[]
): ValidatedDimensionCreationRequest => {
    let datavalues: SourceAssignmentDTO | null = null;
    let footnotes: SourceAssignmentDTO | null = null;
    const dimensions: SourceAssignmentDTO[] = [];
    const ignore: SourceAssignmentDTO[] = [];

    sourceAssignment.map((sourceInfo) => {
        if (!fileImport.sources?.find((source: Source) => source.id === sourceInfo.sourceId)) {
            throw new Error(`Source with id ${sourceInfo.sourceId} not found`);
        }

        switch (sourceInfo.sourceType) {
            case SourceType.DataValues:
                if (datavalues) {
                    throw new Error('Only one DataValues source can be specified');
                }
                datavalues = sourceInfo;
                break;
            case SourceType.FootNotes:
                if (footnotes) {
                    throw new Error('Only one FootNote source can be specified');
                }
                footnotes = sourceInfo;
                break;
            case SourceType.Dimension:
                dimensions.push(sourceInfo);
                break;
            case SourceType.Ignore:
                ignore.push(sourceInfo);
                break;
            default:
                throw new Error(`Invalid source type: ${sourceInfo.sourceType}`);
        }
    });

    return { datavalues, footnotes, dimensions, ignore };
};

async function createUpdateDatavalues(sourceDescriptor: SourceAssignmentDTO) {
    const dataValuesSource = await Source.findOneByOrFail({ id: sourceDescriptor.sourceId });
    dataValuesSource.action = SourceAction.Create;
    dataValuesSource.type = SourceType.DataValues;
    await Source.createQueryBuilder().relation(Source, 'dimension').of(dataValuesSource).set(null);
    await dataValuesSource.save();
}

async function createUpdateIngoredSources(sourceDescriptor: SourceAssignmentDTO) {
    const ignoreSource = await Source.findOneByOrFail({ id: sourceDescriptor.sourceId });
    ignoreSource.action = SourceAction.Ignore;
    ignoreSource.type = SourceType.Ignore;
    await Source.createQueryBuilder().relation(Source, 'dimension').of(ignoreSource).set(null);
    await ignoreSource.save();
}

async function createUpdateFootnotesDimension(
    dataset: Dataset,
    revision: Revision,
    existingDimensions: Dimension[],
    sourceDescriptor: SourceAssignmentDTO
) {
    const footnoteSource = await Source.findOneByOrFail({ id: sourceDescriptor.sourceId });
    if (footnoteSource.type !== SourceType.FootNotes) {
        footnoteSource.type = SourceType.FootNotes;
    }
    const existingFootnotesDimension = existingDimensions.find(
        (dimension) => dimension.type === DimensionType.FootNote
    );
    if (existingFootnotesDimension) {
        footnoteSource.dimension = existingFootnotesDimension;
        await footnoteSource.save();
    } else {
        const footnoteDimension = new Dimension();
        const footnoteDimensionInfo: DimensionInfo[] = [];
        const updateDate = new Date();
        footnoteDimension.dimensionInfo = footnoteDimensionInfo;
        footnoteDimension.type = DimensionType.FootNote;
        footnoteDimension.dataset = dataset;
        footnoteDimension.startRevision = revision;
        footnoteDimension.sources = [footnoteSource];
        await footnoteDimension.save();
        AVAILABLE_LANGUAGES.map(async (lang) => {
            const dimensionInfo = new DimensionInfo();
            dimensionInfo.dimension = footnoteDimension;
            dimensionInfo.language = lang;
            dimensionInfo.name = i18next.t('dimension_info.footnotes.title', { lng: lang });
            dimensionInfo.description = i18next.t('dimension_info.footnotes.description', { lng: lang });
            dimensionInfo.updatedAt = updateDate;
            await dimensionInfo.save();
        });
        await footnoteSource.save();
    }
}

async function createDimension(dataset: Dataset, revision: Revision, sourceDescriptor: SourceAssignmentDTO) {
    const source = await Source.findOneOrFail({
        where: { id: sourceDescriptor.sourceId },
        relations: ['dimension']
    });
    const existingDimension = source.dimension;
    if (existingDimension && source.type === SourceType.Dimension) {
        logger.debug(`No Dimension to create as Source for column ${source.csvField} is already attached to one`);
        return;
    }
    logger.debug("The existing dimension is either a footnotes dimension or we don't have one... So lets create one");
    source.type = SourceType.Dimension;
    source.action = SourceAction.Create;
    await source.save();

    const dimension = new Dimension();
    dimension.type = DimensionType.Raw;
    dimension.dataset = dataset;
    dimension.startRevision = revision;
    dimension.sources = [source];
    source.dimension = dimension;
    const savedDimension = await dimension.save();
    AVAILABLE_LANGUAGES.map(async (lang: string) => {
        const dimensionInfo = new DimensionInfo();
        dimensionInfo.id = savedDimension.id;
        dimensionInfo.dimension = savedDimension;
        dimensionInfo.language = lang;
        dimensionInfo.name = source.csvField;
        await dimensionInfo.save();
    });

    await source.save();
}

async function cleanupDimensions(datasetId: string) {
    const dataset = await Dataset.findOneOrFail({
        where: { id: datasetId },
        relations: ['dimensions', 'dimensions.sources']
    });

    const revisedDimensions = dataset.dimensions;

    for (const dimension of revisedDimensions) {
        const dimensionSources = dimension.sources;
        if (dimensionSources.length === 0) {
            await dimension.remove();
        }
    }
}

export const createDimensionsFromValidatedDimensionRequest = async (
    revision: Revision,
    validatedDimensionCreationRequest: ValidatedDimensionCreationRequest
) => {
    const dataset = revision.dataset;
    if (!dataset) {
        throw new Error('No dataset is attached to this revision');
    }
    const existingDimensions = dataset.dimensions;

    const { datavalues, ignore, footnotes, dimensions } = validatedDimensionCreationRequest;
    if (datavalues) {
        await createUpdateDatavalues(datavalues);
    }

    if (footnotes) {
        await createUpdateFootnotesDimension(dataset, revision, existingDimensions, footnotes);
    }

    await Promise.all(
        dimensions.map(async (dimensionCreationDTO: SourceAssignmentDTO) => {
            await createDimension(dataset, revision, dimensionCreationDTO);
        })
    );

    await Promise.all(
        ignore.map(async (dimensionCreationDTO: SourceAssignmentDTO) => {
            await createUpdateIngoredSources(dimensionCreationDTO);
        })
    );
    await cleanupDimensions(dataset.id);
};
