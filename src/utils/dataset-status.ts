import { isBefore } from 'date-fns';

import { Dataset } from '../entities/dataset/dataset';
import { DatasetStatus } from '../enums/dataset-status';
import { PublishingStatus } from '../enums/publishing-status';

import { Revision } from '../entities/dataset/revision';
import { TaskAction } from '../enums/task-action';
import { TaskStatus } from '../enums/task-status';

export const getDatasetStatus = (dataset: Dataset): DatasetStatus => {
  return dataset.live && isBefore(dataset.live, new Date()) ? DatasetStatus.Live : DatasetStatus.New;
};

export const getPublishingStatus = (dataset: Dataset, revision: Revision): PublishingStatus => {
  const datasetStatus = getDatasetStatus(dataset);
  const openPublishingTask = dataset.tasks?.find((task) => task.open && task.action === TaskAction.Publish);

  if (openPublishingTask) {
    if (openPublishingTask.status === TaskStatus.Requested) {
      return datasetStatus === DatasetStatus.Live
        ? PublishingStatus.UpdatePendingApproval
        : PublishingStatus.PendingApproval;
    }
    if (openPublishingTask.status === TaskStatus.Rejected) return PublishingStatus.ChangesRequested;
  }

  if (datasetStatus === DatasetStatus.New) {
    return revision.approvedAt ? PublishingStatus.Scheduled : PublishingStatus.Incomplete;
  }

  if (revision.approvedAt && revision.publishAt && isBefore(revision.publishAt, new Date())) {
    return PublishingStatus.Published;
  }

  return revision.approvedAt ? PublishingStatus.UpdateScheduled : PublishingStatus.UpdateIncomplete;
};
