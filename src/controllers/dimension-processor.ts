import { DimensionCreationDTO } from '../dtos/dimension-creation-dto';
import { Dataset } from '../entities/dataset';
import { Dimension } from '../entities/dimension';
import { DimensionInfo } from '../entities/dimension-info';
import { DimensionType } from '../enums/dimension-type';
import { Revision } from '../entities/revision';
import { Source } from '../entities/source';
import { SourceType } from '../enums/source-type';
import { i18next, t } from '../middleware/translation';
import { SourceAction } from '../enums/source-action';

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
                case SourceType.DATAVALUES:
                    if (datavalues) {
                        throw new Error('Only one DataValues source can be specified');
                    }
                    source.action = SourceAction.CREATE;
                    datavalues = sourceInfo;
                    break;
                case SourceType.FOOTNOTES:
                    if (footnotes) {
                        throw new Error('Only one FootNote source can be specified');
                    }
                    source.action = SourceAction.CREATE;
                    footnotes = sourceInfo;
                    break;
                case SourceType.DIMENSION:
                    source.action = SourceAction.CREATE;
                    dimensions.push(sourceInfo);
                    break;
                case SourceType.IGNORE:
                    source.action = SourceAction.IGNORE;
                    ignore.push(sourceInfo);
                    break;
                default:
                    throw new Error(`Invalid source type: ${sourceInfo.sourceType}`);
            }
            source.type = sourceInfo.sourceType;
            await source.save();
        })
    );
    return { datavalues, footnotes, dimensions, ignore };
};

export const createDimensions = async (
    revision: Revision,
    validatedDimensionCreationRequest: ValidatedDimensionCreationRequest
): Promise<Dataset> => {
    const dataset = await revision.dataset;
    if (!dataset) {
        throw new Error('No dataset is attached to this revision');
    }
    const languages = i18next.languages;
    const { footnotes, dimensions } = validatedDimensionCreationRequest;
    if (footnotes) {
        const footnoteDimension = new Dimension();
        footnoteDimension.id = crypto.randomUUID().toLowerCase();
        const footnoteDimensionInfo: DimensionInfo[] = [];
        const updateDate = new Date();
        footnoteDimension.dimensionInfo = Promise.resolve(
            languages.map((lang) => {
                const dimensionInfo = new DimensionInfo();
                dimensionInfo.dimension = Promise.resolve(footnoteDimension);
                dimensionInfo.language = lang;
                dimensionInfo.name = t('dimension_info.footnotes.name', { lng: lang });
                dimensionInfo.description = t('dimension_info.footnotes.description', { lng: lang });
                dimensionInfo.updatedAt = updateDate;
                return dimensionInfo;
            })
        );
        footnoteDimension.dimensionInfo = Promise.resolve(footnoteDimensionInfo);
        footnoteDimension.type = DimensionType.FOOTNOTE;
        footnoteDimension.dataset = Promise.resolve(dataset);
        footnoteDimension.startRevision = Promise.resolve(revision);
        const source = await Source.findOne({ where: { id: footnotes.sourceId } });
        if (!source) {
            throw new Error(`Source with id ${footnotes.sourceId} not found`);
        }
        footnoteDimension.sources = Promise.resolve([source]);
        await footnoteDimension.save();
        await source.save();
    }

    await Promise.all(
        dimensions.map(async (dimensionCreationDTO: DimensionCreationDTO) => {
            const dimension = new Dimension();
            dimension.id = crypto.randomUUID().toLowerCase();
            dimension.type = DimensionType.RAW;
            const source = await Source.findOne({ where: { id: dimensionCreationDTO.sourceId } });
            if (!source) {
                throw new Error(`Source with id ${dimensionCreationDTO.sourceId} not found`);
            }
            dimension.dimensionInfo = Promise.resolve(
                languages.map((lang: string) => {
                    const dimensionInfo = new DimensionInfo();
                    dimensionInfo.dimension = Promise.resolve(dimension);
                    dimensionInfo.language = lang;
                    dimensionInfo.name = source.csvField;
                    return dimensionInfo;
                })
            );
            dimension.dataset = Promise.resolve(dataset);
            dimension.startRevision = Promise.resolve(revision);
            dimension.sources = Promise.resolve([source]);
            source.dimension = Promise.resolve(dimension);
            await dimension.save();
            await source.save();
            return dimension;
        })
    );

    const updateDataset = await Dataset.findOneBy({ id: dataset.id });
    if (!updateDataset) {
        throw new Error(`Dataset with id ${dataset.id} not found`);
    }
    return updateDataset;
};
