import { sortBy, last } from 'lodash';

import { Dataset } from '../entities/dataset/dataset';
import { Revision } from '../entities/dataset/revision';
import { FileImport } from '../entities/dataset/file-import';

export const getLatestRevision = (dataset?: Dataset): Revision | undefined => {
    if (!dataset) return undefined;
    return last(sortBy(dataset?.revisions, 'revision_index'));
};

export const getLatestImport = (revision?: Revision): FileImport | undefined => {
    if (!revision) return undefined;
    return last(sortBy(revision?.imports, 'uploaded_at'));
};
