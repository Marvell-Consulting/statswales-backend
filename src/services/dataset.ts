import fs from 'node:fs';

import { RevisionMetadataDTO } from '../dtos/revistion-metadata-dto';
import { TranslationDTO } from '../dtos/translations-dto';
import { Dataset } from '../entities/dataset/dataset';
import { User } from '../entities/user/user';
import { Locale } from '../enums/locale';
import {
  DatasetRepository,
  withDraftAndMetadata,
  withDraftAndProviders,
  withDraftAndTopics,
  withDraftForTasklistState
} from '../repositories/dataset';
import { RevisionRepository } from '../repositories/revision';
import { logger } from '../utils/logger';
import { DataTableAction } from '../enums/data-table-action';
import { RevisionProviderDTO } from '../dtos/revision-provider-dto';
import { RevisionProvider } from '../entities/dataset/revision-provider';
import { RevisionTopic } from '../entities/dataset/revision-topic';
import { DimensionRepository } from '../repositories/dimension';
import { RevisionMetadata } from '../entities/dataset/revision-metadata';
import { outputCube } from '../controllers/cube-controller';
import { DuckdbOutputType } from '../enums/duckdb-outputs';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { isPublished } from '../utils/revision';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { TasklistStateDTO } from '../dtos/tasklist-state-dto';
import { EventLog } from '../entities/event-log';

import { createBaseCube, getCubeTimePeriods } from './cube-handler';
import { uploadCSV } from './csv-processor';
import { DataLakeService } from './datalake';
import { removeAllDimensions, removeMeasure } from './dimension-processor';

export class DatasetService {
  lang: Locale;

  constructor(lang: Locale) {
    this.lang = lang;
  }

  async createNew(title: string, createdBy: User): Promise<Dataset> {
    logger.info(`Creating new dataset...`);

    const dataset = await DatasetRepository.create({ createdBy }).save();
    const firstRev = await RevisionRepository.create({ dataset, createdBy, revisionIndex: 1 }).save();
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

  async updateMetadata(datasetId: string, metadata: RevisionMetadataDTO): Promise<Dataset> {
    const dataset = await DatasetRepository.getById(datasetId, withDraftAndMetadata);
    await RevisionRepository.updateMetadata(dataset.draftRevision!, metadata);

    return DatasetRepository.getById(dataset.id, {});
  }

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

    if (dataset.draftRevision?.revisionIndex === 1) {
      await removeAllDimensions(dataset);
      await removeMeasure(dataset);
    }

    await RevisionRepository.replaceDataTable(dataset.draftRevision!, dataTable);
    await DatasetRepository.replaceFactTable(dataset, dataTable);

    return DatasetRepository.getById(datasetId, {});
  }

  async addDataProvider(datasetId: string, dataProvider: RevisionProviderDTO): Promise<Dataset> {
    const newProvider = RevisionProviderDTO.toRevisionProvider(dataProvider);

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
    const existing = dataset.draftRevision!.revisionProviders;
    const submitted = dataProviders.map((provider) => RevisionProviderDTO.toRevisionProvider(provider));

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
    const revision = dataset.draftRevision!;

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
    const dataset = await DatasetRepository.getById(datasetId, {
      draftRevision: { metadata: true },
      dimensions: { metadata: true }
    });

    const revision = dataset.draftRevision!;
    const dimensions = dataset.dimensions;

    // set all metadata updated_at to the same time, we can use this later to flag untranslated changes
    const now = new Date();

    logger.debug(`Updating dimension name translations...`);
    dimensions.forEach((dimension) => {
      const translation = translations.find((t) => t.type === 'dimension' && t.key === dimension.factTableColumn)!;

      const dimMetaEN = dimension.metadata.find((meta) => meta.language.includes('en'))!;
      dimMetaEN.name = translation?.english || '';
      dimMetaEN.updatedAt = now;

      const dimMetaCY = dimension.metadata.find((meta) => meta.language.includes('cy'))!;
      dimMetaCY.name = translation?.cymraeg || '';
      dimMetaCY.updatedAt = now;
    });

    await DimensionRepository.save(dimensions);

    logger.debug(`Updating metadata translations...`);
    const metaTranslations = translations.filter((t) => t.type === 'metadata');
    const metaEn = revision.metadata.find((meta) => meta.language === Locale.EnglishGb)!;
    const metaCy = revision.metadata.find((meta) => meta.language === Locale.WelshGb)!;

    metaTranslations.forEach((row) => {
      const metaKey = row.key as 'title' | 'summary' | 'collection' | 'quality' | 'roundingDescription';
      metaEn[metaKey] = row.english || '';
      metaCy[metaKey] = row.cymraeg || '';
    });

    metaEn.updatedAt = now;
    metaCy.updatedAt = now;

    await RevisionMetadata.getRepository().save([metaEn, metaCy]);

    logger.debug(`Updating related link translations...`);
    revision.relatedLinks?.forEach((link) => {
      const translation = translations.find((t) => t.type === 'link' && t.key === link.id);
      link.labelEN = translation?.english;
      link.labelCY = translation?.cymraeg;
    });

    await RevisionRepository.save(revision);

    return DatasetRepository.getById(datasetId, {});
  }

  async approveForPublication(datasetId: string, revisionId: string, user: User): Promise<Dataset> {
    const start = performance.now();

    const dataset = await DatasetRepository.getById(datasetId, {});
    const cubeFilePath = await createBaseCube(datasetId, revisionId);
    const periodCoverage = await getCubeTimePeriods(cubeFilePath);

    const dataLakeService = new DataLakeService();
    const cubeBuffer = fs.readFileSync(cubeFilePath);
    const onlineCubeFilename = `${revisionId}.duckdb`;
    await dataLakeService.saveBuffer(onlineCubeFilename, dataset.id, cubeBuffer);

    for (const locale of SUPPORTED_LOCALES) {
      const lang = locale.split('-')[0].toLowerCase();
      logger.debug(`Creating parquet file for language "${lang}" and uploading to data lake`);
      const parquetFilePath = await outputCube(cubeFilePath, lang, DuckdbOutputType.Parquet);
      await dataLakeService.saveBuffer(
        `${revisionId}_${lang}.parquet`,
        dataset.id,
        fs.readFileSync(parquetFilePath)
      );
    }

    const end = performance.now();
    const time = Math.round(end - start);
    logger.info(`Cube and parquet file creation took ${time}ms (including uploading to data lake)`);

    const scheduledRevision = await RevisionRepository.approvePublication(revisionId, onlineCubeFilename, user);
    const approvedDataset = await DatasetRepository.publish(scheduledRevision, periodCoverage);

    return approvedDataset;
  }

  async withdrawFromPublication(datasetId: string, revisionId: string): Promise<Dataset> {
    const revision = await RevisionRepository.withdrawPublication(revisionId);

    if (revision.onlineCubeFilename) {
      const dataLakeService = new DataLakeService();
      await dataLakeService.delete(revision.onlineCubeFilename, datasetId);
    }

    const withdrawnDataset = await DatasetRepository.withdraw(revision);

    return withdrawnDataset;
  }

  async createRevision(datasetId: string, createdBy: User): Promise<Dataset> {
    logger.info(`Creating new revision for dataset: ${datasetId}...`);

    const dataset = await DatasetRepository.findOneOrFail({
      where: { id: datasetId },
      relations: { publishedRevision: true, revisions: true }
    });

    const publishedRevision = dataset.publishedRevision!;
    const unPublishedRevisions = dataset.revisions.filter((rev) => !isPublished(rev));

    if (unPublishedRevisions.length > 0) {
      throw new BadRequestException('errors.create_revision.existing_unpublished_revisions');
    }

    const newRevision = await RevisionRepository.deepCloneRevision(publishedRevision.id, createdBy);
    logger.info(`New draft revision created: ${newRevision.id}`);

    await DatasetRepository.save({ id: datasetId, draftRevision: newRevision, endRevision: newRevision });

    return DatasetRepository.getById(datasetId, withDraftAndMetadata);
  }

  async getTasklistState(datasetId: string, locale: Locale): Promise<TasklistStateDTO> {
    const dataset = await DatasetRepository.getById(datasetId, withDraftForTasklistState);
    const revision = dataset.draftRevision!;

    const translationEvents = await EventLog.getRepository().find({
      where: { entity: 'translations', entityId: revision.id },
      order: { createdAt: 'DESC' }
    });

    return TasklistStateDTO.fromDataset(dataset, revision, locale, translationEvents);
  }
}
