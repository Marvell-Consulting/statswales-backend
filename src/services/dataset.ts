import { FindOptionsRelations } from 'typeorm';

import { RevisionMetadataDTO } from '../dtos/revistion-metadata-dto';
import { TranslationDTO } from '../dtos/translations-dto';
import { Dataset } from '../entities/dataset/dataset';
import { User } from '../entities/user/user';
import { Locale } from '../enums/locale';
import {
    DatasetRepository,
    withDraftForCube,
    withDraftAndMetadata,
    withDraftAndProviders,
    withDraftAndTopics
} from '../repositories/dataset';
import { RevisionRepository } from '../repositories/revision';
import { logger } from '../utils/logger';
import { DataTableAction } from '../enums/data-table-action';
import { RevisionProviderDTO } from '../dtos/revision-provider-dto';
import { RevisionProvider } from '../entities/dataset/revision-provider';
import { RevisionTopic } from '../entities/dataset/revision-topic';

import { uploadCSV } from './csv-processor';

export class DatasetService {
    lang: Locale;
    user: Partial<User> | undefined;

    constructor(lang: Locale, user?: Partial<User>) {
        this.lang = lang;
        this.user = user;
    }

    // Create a new dataaset, first revision and title
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

        return DatasetRepository.getById(dataset.id, withDraftAndMetadata);
    }

    // Patch the metadata for the currently in progress revision
    async updateMetadata(datasetId: string, metadata: RevisionMetadataDTO): Promise<Dataset> {
        const dataset = await DatasetRepository.getById(datasetId, withDraftAndMetadata);
        await RevisionRepository.updateMetadata(dataset.draftRevision, metadata);

        return DatasetRepository.getById(dataset.id, {});
    }

    // Add or replace the fact table for the dataset
    async updateFactTable(datasetId: string, file: Express.Multer.File): Promise<Dataset> {
        const dataset = await DatasetRepository.getById(datasetId, {
            factTable: true,
            draftRevision: { dataTable: true }
        });

        const { buffer, mimetype, originalname } = file;

        logger.debug('Uploading new fact table file to datalake');
        const dataTable = await uploadCSV(buffer, mimetype, originalname, datasetId);

        dataTable.action = DataTableAction.ReplaceAll;
        dataTable.dataTableDescriptions.forEach((col) => {
            col.factTableColumn = col.columnName;
        });

        await RevisionRepository.replaceDataTable(dataset.draftRevision, dataTable);
        await DatasetRepository.replaceFactTable(dataset, dataTable);

        return DatasetRepository.getById(datasetId, withDraftForCube);
    }

    async addDataProvider(datasetId: string, dataProvider: RevisionProviderDTO): Promise<Dataset> {
        const newProvider = RevisionProviderDTO.toRevisionProvider(dataProvider);

        // add new data provider for both languages
        const altLang = newProvider.language.includes(Locale.English) ? Locale.WelshGb : Locale.EnglishGb;

        const newProviderAltLang: Partial<RevisionProvider> = {
            ...newProvider,
            id: undefined,
            language: altLang.toLowerCase()
        };

        await RevisionProvider.getRepository().save([newProvider, newProviderAltLang]);

        logger.debug(`Added new provider for dataset ${datasetId}`);

        return DatasetRepository.getById(datasetId, withDraftAndProviders);
    }

    async updateDataProviders(datasetId: string, dataProviders: RevisionProviderDTO[]): Promise<Dataset> {
        const dataset = await DatasetRepository.getById(datasetId, { draftRevision: { revisionProviders: true } });
        const existing = dataset.draftRevision.revisionProviders;
        const submitted = dataProviders.map((provider) => RevisionProviderDTO.toRevisionProvider(provider));

        // we can receive updates in a single language, but we need to update the relations for both languages

        // work out what providers have been removed and remove for both languages
        const toRemove = existing.filter((existing) => {
            // if the group id is still present in the submitted data then don't remove those providers
            return !submitted.some((submitted) => submitted.groupId === existing.groupId);
        });

        await RevisionProvider.getRepository().remove(toRemove);

        // update the data providers for both languages
        const toUpdate = existing
            .filter((existing) => submitted.some((submitted) => submitted.groupId === existing.groupId))
            .map((updating) => {
                const updated = submitted.find((submitted) => submitted.groupId === updating.groupId)!;
                updating.providerId = updated.providerId;
                updating.providerSourceId = updated.providerSourceId;

                return updating;
            });

        await RevisionProvider.getRepository().save(toUpdate);

        logger.debug(
            `Removed ${toRemove.length} providers and updated ${toUpdate.length} providers for dataset ${datasetId}`
        );

        return DatasetRepository.getById(datasetId, withDraftAndProviders);
    }

    async updateTopics(datasetId: string, topics: string[]): Promise<Dataset> {
        const dataset = await DatasetRepository.getById(datasetId, { draftRevision: { revisionTopics: true } });
        const revision = dataset.draftRevision;

        // remove any existing topic relations
        const existingTopics = revision.revisionTopics;
        await RevisionTopic.getRepository().remove(existingTopics);

        // save the new topic relations
        const newTopics = topics.map((topicId: string) => {
            return RevisionTopic.getRepository().create({ revisionId: revision.id, topicId: parseInt(topicId, 10) });
        });

        await RevisionTopic.getRepository().save(newTopics);

        return DatasetRepository.getById(datasetId, withDraftAndTopics);
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
