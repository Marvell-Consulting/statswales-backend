import { set } from 'lodash';
import { isBefore } from 'date-fns';
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

  const goLiveDate = dataset.live && isBefore(dataset.live, now) ? dataset.live : undefined;

  if (goLiveDate) {
    const goLiveEvent = EventLog.create({
      id: `simulated-${uuid()}`,
      entity: 'dataset',
      action: 'publish',
      createdAt: goLiveDate
    });

    events.push(goLiveEvent);
  }

  return events;
};
