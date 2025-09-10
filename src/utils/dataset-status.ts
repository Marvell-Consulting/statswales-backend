import { isBefore } from 'date-fns';

import { Dataset } from '../entities/dataset/dataset';
import { DatasetStatus } from '../enums/dataset-status';
import { PublishingStatus } from '../enums/publishing-status';

import { Revision } from '../entities/dataset/revision';
import { TaskAction } from '../enums/task-action';
import { TaskStatus } from '../enums/task-status';

export const getDatasetStatus = (dataset: Dataset): DatasetStatus => {
  if (dataset.archivedAt && isBefore(dataset.archivedAt, new Date())) {
    return DatasetStatus.Archived;
  }

  return dataset.firstPublishedAt && isBefore(dataset.firstPublishedAt, new Date())
    ? DatasetStatus.Live
    : DatasetStatus.New;
};

export const getPublishingStatus = (dataset: Dataset, revision: Revision): PublishingStatus => {
  const datasetStatus = getDatasetStatus(dataset);
  const openTasks = dataset.tasks?.filter((task) => task.open) || [];
  const openPublishTask = openTasks.find((task) => task.action === TaskAction.Publish);
  const openUnpublishTask = openTasks.find((task) => task.action === TaskAction.Unpublish);
  const openArchiveTask = openTasks.find((task) => task.action === TaskAction.Archive);
  const openUnarchiveTask = openTasks.find((task) => task.action === TaskAction.Unarchive);

  if (openPublishTask) {
    if (openPublishTask.status === TaskStatus.Requested) {
      return datasetStatus === DatasetStatus.Live
        ? PublishingStatus.UpdatePendingApproval
        : PublishingStatus.PendingApproval;
    }
    if (openPublishTask.status === TaskStatus.Rejected) return PublishingStatus.ChangesRequested;
  }

  if (openUnpublishTask) {
    return PublishingStatus.UnpublishRequested;
  }

  if (openArchiveTask) {
    return PublishingStatus.ArchiveRequested;
  }

  if (openUnarchiveTask) {
    return PublishingStatus.UnarchiveRequested;
  }

  if (datasetStatus === DatasetStatus.New) {
    return revision.approvedAt ? PublishingStatus.Scheduled : PublishingStatus.Incomplete;
  }

  if (revision.approvedAt && revision.publishAt && isBefore(revision.publishAt, new Date())) {
    return PublishingStatus.Published;
  }

  return revision.approvedAt ? PublishingStatus.UpdateScheduled : PublishingStatus.UpdateIncomplete;
};
