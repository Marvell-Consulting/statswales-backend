import { sortBy, last } from 'lodash';

import { Dataset } from '../entities/dataset/dataset';
import { Revision } from '../entities/dataset/revision';
import { FactTable } from '../entities/dataset/fact-table';

export const getLatestRevision = (dataset?: Dataset): Revision | undefined => {
    if (!dataset) return undefined;
    return last(sortBy(dataset?.revisions, 'revision_index'));
};

export const getLatestImport = (revision?: Revision): FactTable | undefined => {
    if (!revision) return undefined;
    return last(sortBy(revision?.factTables, 'uploaded_at'));
};
