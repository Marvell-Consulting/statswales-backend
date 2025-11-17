import { sortBy, last } from 'lodash';

import { Dataset } from '../entities/dataset/dataset';
import { Revision } from '../entities/dataset/revision';

// @deprecated loading all revisions just to get the last one is expensive, just hydrate dataset.endRevision instead
// or just use dataset.endRevisionId directly if you don't need the full object
export const getLatestRevision = (dataset?: Dataset): Revision | undefined => {
  if (!dataset) return undefined;
  return last(sortBy(dataset?.revisions, 'createdAt'));
};
