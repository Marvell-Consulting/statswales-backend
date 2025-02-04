import { sortBy, last } from 'lodash';

import { Dataset } from '../entities/dataset/dataset';
import { Revision } from '../entities/dataset/revision';
import { DataTable } from '../entities/dataset/data-table';

export const getLatestRevision = (dataset?: Dataset): Revision | undefined => {
    if (!dataset) return undefined;
    return last(sortBy(dataset?.revisions, 'createdAt'));
};
