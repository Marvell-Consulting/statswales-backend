import { DimensionCreationDTO } from '../dtos/dimension-creation-dto';
import { Dataset } from '../entities/dataset/dataset';
import { Dimension } from '../entities/dataset/dimension';
import { DimensionInfo } from '../entities/dataset/dimension-info';
import { DimensionType } from '../enums/dimension-type';
import { Revision } from '../entities/dataset/revision';
import { Source } from '../entities/dataset/source';
import { SourceType } from '../enums/source-type';
import { AVAILABLE_LANGUAGES, i18next } from '../middleware/translation';
import { SourceAction } from '../enums/source-action';
import { logger } from '../utils/logger';

export interface ValidatedDimensionCreationRequest {
    datavalues: DimensionCreationDTO | null;
    footnotes: DimensionCreationDTO | null;
    dimensions: DimensionCreationDTO[];
    ignore: DimensionCreationDTO[];
}

export const validateDimensionCreationRequest = async (
    dimensionCreationDTO: DimensionCreationDTO[]
): Promise<ValidatedDimensionCreationRequest> => {
    let datavalues: DimensionCreationDTO | null = null;
    let footnotes: DimensionCreationDTO | null = null;
    const dimensions: DimensionCreationDTO[] = [];
    const ignore: DimensionCreationDTO[] = [];
    await Promise.all(
        dimensionCreationDTO.map(async (sourceInfo) => {
            const source = await Source.findOne({ where: { id: sourceInfo.sourceId } });
            if (!source) {
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
        })
    );
    return { datavalues, footnotes, dimensions, ignore };
};

async function createUpdateDatavalues(sourceDescriptor: DimensionCreationDTO) {
    const dataValuesSource = await Source.findOneByOrFail({ id: sourceDescriptor.sourceId });
    dataValuesSource.action = SourceAction.Create;
    dataValuesSource.type = SourceType.DataValues;
    await Source.createQueryBuilder().relation(Source, 'dimension').of(dataValuesSource).set(null);
    await dataValuesSource.save();
}

async function createUpdateIngoredSources(sourceDescriptor: DimensionCreationDTO) {
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
    sourceDescriptor: DimensionCreationDTO
) {
    const footnoteSource = await Source.findOneByOrFail({ id: sourceDescriptor.sourceId });
    if (footnoteSource.type !== SourceType.FootNotes) {
        footnoteSource.type = SourceType.FootNotes;
    }
    const existingFootnotesDimension = existingDimensions.find(
        (dimension) => dimension.type === DimensionType.FootNote
    );
    if (existingFootnotesDimension) {
        footnoteSource.dimension = Promise.resolve(existingFootnotesDimension);
        await footnoteSource.save();
    } else {
        const footnoteDimension = new Dimension();
        const footnoteDimensionInfo: DimensionInfo[] = [];
        const updateDate = new Date();
        footnoteDimension.dimensionInfo = Promise.resolve(footnoteDimensionInfo);
        footnoteDimension.type = DimensionType.FootNote;
        footnoteDimension.dataset = Promise.resolve(dataset);
        footnoteDimension.startRevision = Promise.resolve(revision);
        footnoteDimension.sources = Promise.resolve([footnoteSource]);
        await footnoteDimension.save();
        AVAILABLE_LANGUAGES.map(async (lang) => {
            const dimensionInfo = new DimensionInfo();
            dimensionInfo.dimension = Promise.resolve(footnoteDimension);
            dimensionInfo.language = lang;
            dimensionInfo.name = i18next.t('dimension_info.footnotes.title', { lng: lang });
            dimensionInfo.description = i18next.t('dimension_info.footnotes.description', { lng: lang });
            dimensionInfo.updatedAt = updateDate;
            await dimensionInfo.save();
        });
        await footnoteSource.save();
    }
}

async function createDimension(
    dataset: Dataset,
    revision: Revision,
    existingDimensions: Dimension[],
    sourceDescriptor: DimensionCreationDTO
) {
    const checkSource = await Source.findOneByOrFail({ id: sourceDescriptor.sourceId });
    const existingDimension = await checkSource.dimension;
    if (existingDimension && checkSource.type === SourceType.Dimension) {
        logger.debug(`No Dimension to create as Source for column ${checkSource.csvField} is already attached to one`);
        return;
    }
    logger.debug("The existing dimension is either a footnotes dimension or we don't have one... So lets create one");
    const source = await Source.findOneByOrFail({ id: sourceDescriptor.sourceId });
    source.type = SourceType.Dimension;
    source.action = SourceAction.Create;
    await source.save();
    const dimension = new Dimension();
    dimension.type = DimensionType.Raw;
    dimension.dataset = Promise.resolve(dataset);
    dimension.startRevision = Promise.resolve(revision);
    dimension.sources = Promise.resolve([source]);
    source.dimension = Promise.resolve(dimension);
    const savedDimension = await dimension.save();
    AVAILABLE_LANGUAGES.map(async (lang: string) => {
        const dimensionInfo = new DimensionInfo();
        dimensionInfo.id = savedDimension.id;
        dimensionInfo.dimension = Promise.resolve(savedDimension);
        dimensionInfo.language = lang;
        dimensionInfo.name = source.csvField;
        await dimensionInfo.save();
    });
    await source.save();
}

async function cleanupDimension(dataset: Dataset) {
    const updateDataset = await Dataset.findOneByOrFail({ id: dataset.id });
    const revisedDimensions = await updateDataset.dimensions;
    for (const dimension of revisedDimensions) {
        const dimensionSources = await dimension.sources;
        if (dimensionSources.length === 0) {
            await dimension.remove();
        }
    }
}

export const createDimensionsFromValidatedDimensionRequest = async (
    revision: Revision,
    validatedDimensionCreationRequest: ValidatedDimensionCreationRequest
) => {
    const dataset = await revision.dataset;
    if (!dataset) {
        throw new Error('No dataset is attached to this revision');
    }
    const existingDimensions = await dataset.dimensions;

    const { datavalues, ignore, footnotes, dimensions } = validatedDimensionCreationRequest;
    if (datavalues) {
        await createUpdateDatavalues(datavalues);
    }

    if (footnotes) {
        await createUpdateFootnotesDimension(dataset, revision, existingDimensions, footnotes);
    }

    await Promise.all(
        dimensions.map(async (dimensionCreationDTO: DimensionCreationDTO) => {
            await createDimension(dataset, revision, existingDimensions, dimensionCreationDTO);
        })
    );

    await Promise.all(
        ignore.map(async (dimensionCreationDTO: DimensionCreationDTO) => {
            await createUpdateIngoredSources(dimensionCreationDTO);
        })
    );
    await cleanupDimension(dataset);
};
