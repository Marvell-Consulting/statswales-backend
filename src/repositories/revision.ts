import { Not, IsNull } from 'typeorm';

import { dataSource } from '../db/data-source';
import { logger } from '../utils/logger';
import { Dataset } from '../entities/dataset/dataset';
import { FileImport } from '../entities/dataset/file-import';
import { Revision } from '../entities/dataset/revision';
import { User } from '../entities/user/user';

export const RevisionRepository = dataSource.getRepository(Revision).extend({
    async createFromImport(dataset: Dataset, fileImport: FileImport, user: User): Promise<Revision> {
        logger.debug(`Creating new Revision for Dataset "${dataset.id}" from Import "${fileImport.id}"...`);

        const unpublishedRevisions = await dataSource
            .getRepository(Revision)
            .find({ where: { dataset: { id: dataset.id }, publishAt: IsNull() } });

        // purge any previous unpublished revisions and associated imports / sources / dimensions
        await dataSource.getRepository(Revision).remove(unpublishedRevisions);

        const lastPublishedRevision = await dataSource.getRepository(Revision).findOne({
            where: {
                dataset: { id: dataset.id },
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
                imports: [fileImport],
                createdBy: user
            })
            .save();

        return newRevision;
    }
});
