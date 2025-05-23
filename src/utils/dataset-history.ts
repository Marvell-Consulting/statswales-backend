import { set } from 'lodash';
import { isAfter, isBefore } from 'date-fns';
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

export const injectSimulatedEvents = (dataset: Dataset, history: EventLog[]): EventLog[] => {
  const now = new Date();
  let goLiveDate = dataset.live && isBefore(dataset.live, now) ? dataset.live : undefined;

  return history.reduce((events: EventLog[], event) => {
    if (goLiveDate && isAfter(event.createdAt, now)) {
      const goLiveEvent = EventLog.create({
        id: `simulated-${uuid()}`,
        entity: 'dataset',
        action: 'publish',
        createdAt: goLiveDate,
        data: {
          datasetId: dataset.id
        }
      });
      events.push(goLiveEvent);
      goLiveDate = undefined;
    }

    events.push(event);
    return events;
  }, [] as EventLog[]);
};
