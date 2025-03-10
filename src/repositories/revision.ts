import { FindOptionsRelations, FindOneOptions } from 'typeorm';
import { has, pick } from 'lodash';

import { dataSource } from '../db/data-source';
import { logger } from '../utils/logger';
import { DataTable } from '../entities/dataset/data-table';
import { Revision } from '../entities/dataset/revision';
import { User } from '../entities/user/user';
import { RevisionMetadata } from '../entities/dataset/revision-metadata';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { Locale } from '../enums/locale';
import { RevisionMetadataDTO } from '../dtos/revistion-metadata-dto';
import { RevisionProvider } from '../entities/dataset/revision-provider';
import { RevisionTopic } from '../entities/dataset/revision-topic';

import { DataTableRepository } from './data-table';

export const withDataTable: FindOptionsRelations<Revision> = {
  dataTable: {
    dataTableDescriptions: true
  }
};

export const withMetadata: FindOptionsRelations<Revision> = {
  metadata: true
};

export const RevisionRepository = dataSource.getRepository(Revision).extend({
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
      await DataTableRepository.remove(revision.dataTable);
    }

    const updatedRevision = await this.merge(revision, { dataTable }).save();

    return updatedRevision;
  },

  async createMetadata(revision: Revision, title: string, lang: string): Promise<Revision> {
    logger.debug(`Creating metadata for supported locales for revision '${revision.id}'...`);

    const metadata: RevisionMetadata[] = SUPPORTED_LOCALES.map((language: Locale) => {
      return RevisionMetadata.create({ revision, language, title: language === lang ? title : '' });
    });

    await dataSource.getRepository(RevisionMetadata).save(metadata);

    return this.getById(revision.id, { metadata: true });
  },

  async updateMetadata(revision: Revision, metaDto: RevisionMetadataDTO): Promise<Revision> {
    logger.debug(`Updating revision metadata for lang '${metaDto.language}'`);

    const splitMeta = RevisionMetadataDTO.splitMeta(metaDto);

    // props that aren't translated live on the revision itself
    await this.merge(revision, splitMeta.revision).save();

    // props that are translated live in revision metadata
    const metaRepo = dataSource.getRepository(RevisionMetadata);
    const existingMeta: RevisionMetadata = await metaRepo.findOneOrFail({
      where: { id: revision.id, language: metaDto.language }
    });
    await metaRepo.merge(existingMeta, splitMeta.metadata).save();

    return this.getById(revision.id, { metadata: true });
  },

  async updatePublishDate(revision: Revision, publishAt: Date): Promise<Revision> {
    logger.debug(`Updating publish date for revision '${revision.id}'`);
    revision.publishAt = publishAt;
    return this.save(revision);
  },

  async approvePublication(revisionId: string, onlineCubeFilename: string, approver: User): Promise<Revision> {
    const scheduledRevision = await dataSource.getRepository(Revision).findOneOrFail({
      where: { id: revisionId },
      relations: { dataset: true }
    });

    if (scheduledRevision.revisionIndex === 0) {
      const result = await dataSource.query(
        `SELECT MAX(revision_index) AS max_index FROM revision WHERE dataset_id = $1 AND approved_at IS NOT NULL;`,
        [scheduledRevision.dataset.id]
      );

      const highestIndex = result[0].max_index;
      scheduledRevision.revisionIndex = highestIndex + 1;
    }

    scheduledRevision.approvedAt = new Date();
    scheduledRevision.approvedBy = approver;
    scheduledRevision.onlineCubeFilename = onlineCubeFilename;
    await scheduledRevision.save();

    return scheduledRevision;
  },

  async withdrawPublication(revisionId: string): Promise<Revision> {
    const approvedRevision = await dataSource.getRepository(Revision).findOneOrFail({
      where: { id: revisionId },
      relations: { dataset: true }
    });

    approvedRevision.approvedAt = null;
    approvedRevision.approvedBy = null;
    approvedRevision.onlineCubeFilename = null;
    await approvedRevision.save();

    if (approvedRevision.revisionIndex === 1) {
      approvedRevision.dataset.live = null;
      await approvedRevision.dataset.save();
    }

    return approvedRevision;
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

    return this.create({
      ...copyProps,
      previousRevisionId: prevRevision.id,
      revisionIndex: 0,
      createdBy,
      metadata,
      revisionProviders,
      revisionTopics
    }).save();
  }
});
