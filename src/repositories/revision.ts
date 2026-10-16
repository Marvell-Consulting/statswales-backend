import { FindOptionsRelations, FindOneOptions } from 'typeorm';
import { has, pick } from 'lodash';

import { publisherDataSource } from '../db/publisher-source';
import { logger } from '../utils/logger';
import { DataTable } from '../entities/dataset/data-table';
import { Revision } from '../entities/dataset/revision';
import { User } from '../entities/user/user';
import { RevisionMetadata } from '../entities/dataset/revision-metadata';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { Locale } from '../enums/locale';
import { RevisionMetadataDTO } from '../dtos/revision-metadata-dto';
import { RevisionProvider } from '../entities/dataset/revision-provider';
import { RevisionTopic } from '../entities/dataset/revision-topic';

import { BadRequestException } from '../exceptions/bad-request.exception';
import { getPublishingStatus } from '../utils/dataset-status';
import { PublishingStatus } from '../enums/publishing-status';

export const withDataTable: FindOptionsRelations<Revision> = {
  dataTable: {
    dataTableDescriptions: true
  }
};

export const withMetadata: FindOptionsRelations<Revision> = {
  metadata: true
};

export const withMetadataAndProviders: FindOptionsRelations<Revision> = {
  metadata: true,
  revisionProviders: { provider: true, providerSource: true }
};

export const withMetadataProvidersAndTopic: FindOptionsRelations<Revision> = {
  metadata: true,
  revisionProviders: { provider: true, providerSource: true },
  revisionTopics: { topic: true }
};

export type RevisionList = {
  id: string;
  dataset_id: string;
  data_table_id: string | null;
  revision_index: number;
};

export const RevisionRepository = publisherDataSource.getRepository(Revision).extend({
  async getById(id: string, relations: FindOptionsRelations<Revision> = withDataTable): Promise<Revision> {
    const findOptions: FindOneOptions<Revision> = { where: { id }, relations };

    if (has(relations, 'dataTable.dataTableDescriptions')) {
      // sort sources by column index if they're requested
      findOptions.order = {
        dataTable: { dataTableDescriptions: { columnIndex: 'ASC' } }
      };
    }

    return this.findOneOrFail(findOptions);
  },

  async replaceDataTable(revision: Revision, dataTable: DataTable): Promise<Revision> {
    logger.debug(`Updating dataTable for revision '${revision.id}'`);

    if (revision.dataTable) {
      logger.debug(`Existing dataTable '${revision.dataTable.id}' for revision '${revision.id}' found, deleting`);
      await this.manager.getRepository(DataTable).remove(revision.dataTable);
    }

    return this.save(this.merge(revision, { dataTable }));
  },

  async createMetadata(revision: Revision, title: string, lang: string): Promise<Revision> {
    logger.debug(`Creating metadata for supported locales for revision '${revision.id}'...`);

    const metadata: RevisionMetadata[] = SUPPORTED_LOCALES.map((language: Locale) => {
      return RevisionMetadata.create({ revision, language, title: language === lang ? title : '' });
    });

    await this.manager.getRepository(RevisionMetadata).save(metadata);

    return this.getById(revision.id, { metadata: true });
  },

  async updateMetadata(revision: Revision, metaDto: RevisionMetadataDTO): Promise<Revision> {
    logger.debug(`Updating revision metadata for lang '${metaDto.language}'`);

    const splitMeta = RevisionMetadataDTO.splitMeta(metaDto);

    // props that aren't translated live on the revision itself
    await this.save(this.merge(revision, splitMeta.revision));

    // props that are translated live in revision metadata
    const metaRepo = this.manager.getRepository(RevisionMetadata);
    const existingMeta: RevisionMetadata = await metaRepo.findOneOrFail({
      where: { id: revision.id, language: metaDto.language }
    });
    await metaRepo.save(metaRepo.merge(existingMeta, splitMeta.metadata));

    return this.getById(revision.id, { metadata: true });
  },

  async updatePublishDate(revision: Revision, publishAt: Date): Promise<Revision> {
    logger.debug(`Updating publish date for revision '${revision.id}'`);
    revision.publishAt = publishAt;
    return this.save(revision);
  },

  async approvePublication(revisionId: string, onlineCubeFilename: string, approver: User): Promise<Revision> {
    const scheduledRevision = await this.findOneOrFail({
      where: { id: revisionId },
      relations: { dataset: true }
    });

    if (scheduledRevision.revisionIndex === 0) {
      const result = await this.query(
        `SELECT MAX(revision_index) AS max_index FROM revision WHERE dataset_id = $1 AND approved_at IS NOT NULL;`,
        [scheduledRevision.dataset.id]
      );

      const highestIndex = result[0].max_index;
      scheduledRevision.revisionIndex = highestIndex + 1;
    }

    scheduledRevision.approvedAt = new Date();
    scheduledRevision.approvedBy = approver;
    scheduledRevision.onlineCubeFilename = onlineCubeFilename;

    return this.save(scheduledRevision);
  },

  async getAllRevisionIds(): Promise<RevisionList[]> {
    return this.query('SELECT id, dataset_id, data_table_id, revision_index FROM revision');
  },

  async getAllDraftRevisionIds(): Promise<RevisionList[]> {
    return this.query('SELECT id, dataset_id, data_table_id, revision_index FROM revision WHERE approved_at IS NULL');
  },

  // returns the reverted revision and the online cube filename it held, as the caller is responsible for deleting
  // the file (outside any transaction) once the revert has been committed
  async revertToDraft(revisionId: string): Promise<{ revision: Revision; removedCubeFilename: string | null }> {
    const revision = await this.findOneOrFail({
      where: { id: revisionId },
      relations: { dataset: true }
    });

    if (getPublishingStatus(revision.dataset, revision) === PublishingStatus.Published) {
      throw new BadRequestException('errors.withdraw.already_published');
    }

    const dataset = revision.dataset;

    if (revision.revisionIndex === 1) {
      dataset.firstPublishedAt = null;
    } else {
      revision.revisionIndex = 0;
    }

    const removedCubeFilename = revision.onlineCubeFilename ?? null;

    revision.approvedAt = null;
    revision.approvedBy = null;
    revision.onlineCubeFilename = null;
    await this.save(revision);

    dataset.draftRevisionId = revision.id;
    dataset.publishedRevisionId = revision.previousRevisionId;

    await this.manager.save(dataset);

    return { revision, removedCubeFilename };
  },

  async deepCloneRevision(revisionId: string, createdBy: User): Promise<Revision> {
    const prevRevision = await this.getById(revisionId, {
      dataTable: true,
      metadata: true,
      revisionProviders: true,
      revisionTopics: true
    });

    const copyProps = pick(prevRevision, [
      'datasetId',
      'roundingApplied',
      'updateFrequency',
      'designation',
      'relatedLinks'
    ]);

    const metadata = prevRevision.metadata.map((meta) =>
      RevisionMetadata.create({
        ...meta,
        reason: undefined,
        revision: undefined,
        createdAt: undefined,
        updatedAt: undefined
      })
    );

    const revisionProviders = prevRevision.revisionProviders.map((revProvider) =>
      RevisionProvider.create({
        ...revProvider,
        id: undefined,
        revision: undefined,
        createdAt: undefined
      })
    );

    const revisionTopics = prevRevision.revisionTopics.map((revTopic) =>
      RevisionTopic.create({
        topicId: revTopic.topicId
      })
    );

    return this.save(
      this.create({
        ...copyProps,
        previousRevisionId: prevRevision.id,
        revisionIndex: 0,
        createdBy,
        metadata,
        revisionProviders,
        revisionTopics
      })
    );
  }
});
