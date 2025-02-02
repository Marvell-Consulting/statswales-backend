import { Not, IsNull } from 'typeorm';

import { dataSource } from '../db/data-source';
import { logger } from '../utils/logger';
import { Dataset } from '../entities/dataset/dataset';
import { FactTable } from '../entities/dataset/fact-table';
import { Revision } from '../entities/dataset/revision';
import { User } from '../entities/user/user';

export const RevisionRepository = dataSource.getRepository(Revision).extend({
    async createFromImport(dataset: Dataset, fileImport: FactTable, user: User): Promise<Revision> {
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
                revisionIndex: (lastPublishedRevision?.revisionIndex || 0) + 1,
                factTables: [fileImport],
                createdBy: user
            })
            .save();

        return newRevision;
    },

    async updatePublishDate(revision: Revision, publishAt: Date): Promise<Revision> {
        logger.debug(`Updating Publish Date for Revision "${revision.id}"...`);
        revision.publishAt = publishAt;
        return dataSource.getRepository(Revision).save(revision);
    },

    async approvePublication(datasetId: string, approver: User): Promise<Revision> {
        const scheduledRevision = await dataSource.getRepository(Revision).findOneOrFail({
            where: {
                dataset: { id: datasetId },
                approvedAt: IsNull(),
                publishAt: Not(IsNull())
            },
            order: { revisionIndex: 'DESC' },
            relations: { dataset: true }
        });

        scheduledRevision.approvedAt = new Date();
        scheduledRevision.approvedBy = approver;
        await scheduledRevision.save();

        if (scheduledRevision.revisionIndex === 1) {
            scheduledRevision.dataset.live = scheduledRevision.publishAt;
            await scheduledRevision.dataset.save();
        }

        return scheduledRevision;
    },

    async withdrawPublication(datasetId: string): Promise<Revision> {
        const approvedRevision = await dataSource.getRepository(Revision).findOneOrFail({
            where: {
                dataset: { id: datasetId },
                approvedAt: Not(IsNull()),
                publishAt: Not(IsNull())
            },
            order: { revisionIndex: 'DESC' },
            relations: { dataset: true }
        });

        approvedRevision.approvedAt = null;
        approvedRevision.approvedBy = null;
        await approvedRevision.save();

        if (approvedRevision.revisionIndex === 1) {
            approvedRevision.dataset.live = null;
            await approvedRevision.dataset.save();
        }

        return approvedRevision;
    }
});
