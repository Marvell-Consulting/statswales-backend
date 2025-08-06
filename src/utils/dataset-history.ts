import { set } from 'lodash';
import { addSeconds, isBefore, max } from 'date-fns';
import { v4 as uuid } from 'uuid';

import { Dataset } from '../entities/dataset/dataset';
import { EventLog } from '../entities/event-log';

export const flagUpdateTask = (dataset: Dataset, event: EventLog): EventLog => {
  if (event.entity === 'task') {
    set(event, 'data.isUpdate', dataset.startRevisionId !== event.data?.metadata?.revisionId);
  }
  return event;
};

export const omitDatasetUpdates = (event: EventLog): boolean => {
  if (event.entity === 'dataset' && event.action === 'update') return false;
  return true;
};

export const omitRevisionUpdates = (event: EventLog): boolean => {
  // ignore first revision creation
  if (event.entity === 'revision' && event.action === 'insert' && event.data?.revisionIndex === 1) return false;

  // ignore revision updates
  if (event.entity === 'revision' && event.action === 'update') return false;

  return true;
};

export const generateSimulatedEvents = (dataset: Dataset): EventLog[] => {
  const events: EventLog[] = [];
  const now = new Date();

  const firstPublished = dataset.live && isBefore(dataset.live, now) ? dataset.live : undefined;
  const firstRev = dataset.revisions?.find((rev) => rev.id === dataset.startRevisionId);

  if (firstPublished && firstRev) {
    // make sure the "first published" log entry appears after the first revision was approved
    const firstPublishedDate = addSeconds(max([firstRev.approvedAt!, dataset.live!]), 1);

    const goLiveEvent = EventLog.create({
      id: `simulated-${uuid()}`,
      entity: 'dataset',
      action: 'publish',
      createdAt: firstPublishedDate,
      user: { name: 'system' }
    });

    events.push(goLiveEvent);
  }

  dataset.revisions?.forEach((revision) => {
    if (revision.revisionIndex > 1 && revision.publishAt && isBefore(revision.publishAt, now)) {
      // make sure the "update published" log entry appears after the revision was approved
      const revisionPublishedDate = addSeconds(max([revision.approvedAt!, revision.publishAt]), 1);

      const revisionPublishedEvent = EventLog.create({
        id: `simulated-${uuid()}`,
        entity: 'revision',
        action: 'publish',
        data: { revisionIndex: revision.revisionIndex },
        createdAt: revisionPublishedDate,
        user: { name: 'system' }
      });
      events.push(revisionPublishedEvent);
    }
  });

  return events;
};
