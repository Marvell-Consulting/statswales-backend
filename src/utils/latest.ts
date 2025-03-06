import { sortBy, last } from 'lodash';

import { Dataset } from '../entities/dataset/dataset';
import { Revision } from '../entities/dataset/revision';

export const getLatestRevision = (dataset?: Dataset): Revision | undefined => {
    if (!dataset) return undefined;
    return last(sortBy(dataset?.revisions, 'createdAt'));
};
