import { FindOptionsRelations } from 'typeorm';

import { RevisionMetadataDTO } from '../dtos/revistion-metadata-dto';
import { TranslationDTO } from '../dtos/translations-dto';
import { Dataset } from '../entities/dataset/dataset';
import { DimensionMetadata } from '../entities/dataset/dimension-metadata';
import { User } from '../entities/user/user';
import { Locale } from '../enums/locale';
import { DatasetRepository } from '../repositories/dataset';
import { RevisionRepository } from '../repositories/revision';
import { logger } from '../utils/logger';

export class DatasetService {
    lang: Locale;
    user: Partial<User> | undefined;

    constructor(lang: Locale, user?: Partial<User>) {
        this.lang = lang;
        this.user = user;
    }

    async createNew(title: string): Promise<Dataset> {
        logger.info(`Creating new dataset...`);

        const dataset = await DatasetRepository.create({ createdBy: this.user }).save();
        const firstRev = await RevisionRepository.create({ dataset, createdBy: this.user, revisionIndex: 1 }).save();
        await RevisionRepository.createMetadata(firstRev, title, this.lang);

        await DatasetRepository.save({
            ...dataset,
            draftRevision: firstRev,
            startRevision: firstRev,
            endRevision: firstRev
        });

        logger.info(`Dataset '${dataset.id}' created with draft revision '${firstRev.id}'`);

        return DatasetRepository.getById(dataset.id, { draftRevision: { metadata: true } });
    }

    async updateMetadata(datasetId: string, metadata: RevisionMetadataDTO): Promise<Dataset> {
        const dataset = await DatasetRepository.getById(datasetId, { draftRevision: { metadata: true } });
        await RevisionRepository.updateMetadata(dataset.draftRevision, metadata);

        return DatasetRepository.getById(dataset.id, {});
    }

    async updateTranslations(datasetId: string, translations: TranslationDTO[]): Promise<Dataset> {
        const translatedRelations: FindOptionsRelations<Dataset> = {
            draftRevision: { metadata: true },
            dimensions: { metadata: true }
        };

        const dataset = await DatasetRepository.getById(datasetId, translatedRelations);

        const revision = dataset.draftRevision;
        const dimensionTranslations = translations.filter((t) => t.type === 'dimension');

        const dimensions = dataset.dimensions;

        // logger.debug(`Updating dimension names...`);

        // for (const row of dimensionTranslations) {
        //     const englishDimInfo = await dimensionMetaRepo.findOneByOrFail({ id: row.id, language: Locale.EnglishGb });
        //     englishDimInfo.name = row.english || '';
        //     await englishDimInfo.save();

        //     const welshDimInfo = await dimensionMetaRepo.findOneByOrFail({ id: row.id, language: Locale.WelshGb });
        //     welshDimInfo.name = row.cymraeg || '';
        //     await welshDimInfo.save();
        // }

        const metaTranslations = translations.filter((t) => t.type === 'metadata');

        logger.debug(`Updating metadata...`);

        const metaEn = revision.metadata.find((meta) => meta.language === Locale.EnglishGb)!;
        const metaCy = revision.metadata.find((meta) => meta.language === Locale.WelshGb)!;

        metaTranslations.forEach((row) => {
            const metaKey = row.key as 'title' | 'summary' | 'collection' | 'quality' | 'roundingDescription';
            metaEn[metaKey] = row.english || '';
            metaCy[metaKey] = row.cymraeg || '';
        });

        await metaEn.save();
        await metaCy.save();

        return DatasetRepository.getById(datasetId, translatedRelations);
    }
}
