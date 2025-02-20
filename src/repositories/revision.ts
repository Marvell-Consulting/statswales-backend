import { Not, IsNull, FindOptionsRelations, FindOneOptions } from 'typeorm';
import { has } from 'lodash';

import { dataSource } from '../db/data-source';
import { logger } from '../utils/logger';
import { DataTable } from '../entities/dataset/data-table';
import { Revision } from '../entities/dataset/revision';
import { User } from '../entities/user/user';
import { Dataset } from '../entities/dataset/dataset';
import { RevisionMetadata } from '../entities/dataset/revision-metadata';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { Locale } from '../enums/locale';
import { RevisionMetadataDTO } from '../dtos/revistion-metadata-dto';

const defaultRelations: FindOptionsRelations<Revision> = {
    createdBy: true,
    dataTable: {
        dataTableDescriptions: true
    }
};

export const RevisionRepository = dataSource.getRepository(Revision).extend({
    async getById(id: string, relations: FindOptionsRelations<Revision> = defaultRelations): Promise<Revision> {
        const findOptions: FindOneOptions<Revision> = { where: { id }, relations };
        logger.debug(
            `Getting Revision by ID "${id}" with the following relations: ${JSON.stringify(relations, null, 2)}`
        );

        if (has(relations, 'revisions.factTables.factTableInfo')) {
            // sort sources by column index if they're requested
            findOptions.order = {
                dataTable: { dataTableDescriptions: { columnIndex: 'ASC' } }
            };
        }

        return this.findOneOrFail(findOptions);
    },

    async createFromImport(dataset: Dataset, fileImport: DataTable, user: User): Promise<Revision> {
        logger.debug(`Creating new Revision for Dataset "${dataset.id}" from FactTable "${fileImport.id}"...`);

        const unpublishedRevisions = await dataSource
            .getRepository(Revision)
            .find({ where: { dataset: { id: dataset.id }, publishAt: IsNull() } });

        // purge any previous unpublished revisions and associated imports / sources / dimensions
        await dataSource.getRepository(Revision).remove(unpublishedRevisions);

        const lastPublishedRevision = await dataSource.getRepository(Revision).findOne({
            where: {
                dataset: { id: dataset.id },
                revisionIndex: Not(IsNull()),
                approvedAt: Not(IsNull()),
                publishAt: Not(IsNull())
            },
            order: { revisionIndex: 'DESC' }
        });

        const newRevision = await dataSource
            .getRepository(Revision)
            .create({
                dataset,
                previousRevision: lastPublishedRevision || undefined,
                revisionIndex: 1,
                dataTable: fileImport,
                createdBy: user
            })
            .save();

        return newRevision;
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

        const highestIndex = await dataSource.query(
            `SELECT MAX(revision_index) AS max_index FROM revision WHERE dataset_id = $1 AND approved_at IS NOT NULL;`,
            [scheduledRevision.dataset.id]
        );
        if (highestIndex[0].max_index !== null) {
            scheduledRevision.revisionIndex = highestIndex[0].max_index + 1;
        }

        scheduledRevision.approvedAt = new Date();
        scheduledRevision.approvedBy = approver;
        scheduledRevision.onlineCubeFilename = onlineCubeFilename;
        await scheduledRevision.save();

        if (scheduledRevision.revisionIndex === 1) {
            scheduledRevision.dataset.live = scheduledRevision.publishAt;
            await scheduledRevision.dataset.save();
        }

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
    }
});
