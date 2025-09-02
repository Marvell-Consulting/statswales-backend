import { Dataset } from '../entities/dataset/dataset';
import { DimensionUpdateTask } from '../interfaces/revision-task';

export async function updateRevisionTasks(dataset: Dataset, id: string, type: 'dimension' | 'measure'): Promise<void> {
  if (dataset.draftRevision && dataset.draftRevision?.revisionIndex != 1) {
    const revision = dataset.draftRevision;
    const task: DimensionUpdateTask = { id, lookupTableUpdated: true };
    if (!dataset.draftRevision.tasks) {
      if (type === 'dimension') {
        revision.tasks = { dimensions: [task], measure: undefined };
      } else {
        revision.tasks = { dimensions: [], measure: task };
      }
    } else {
      const tasks = dataset.draftRevision.tasks;
      if (type === 'dimension') {
        const currentTask = tasks.dimensions.find((task) => task.id === id);
        if (currentTask) {
          tasks.dimensions.forEach((task) => {
            if (task.id === id) task.lookupTableUpdated = true;
          });
        } else {
          tasks.dimensions.push(task);
        }
      } else {
        if (tasks.measure) {
          tasks.measure.lookupTableUpdated = true;
        } else {
          tasks.measure = task;
        }
      }
      revision.tasks = tasks;
    }
    await revision.save();
  }
}
