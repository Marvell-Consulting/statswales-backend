import { Task, TaskMetadata } from '../entities/task/task';
import { TaskAction } from '../enums/task-action';
import { TaskStatus } from '../enums/task-status';

export class TaskDTO {
  id: string;
  action: TaskAction;
  status: TaskStatus;
  open: boolean;
  dataset_id?: string;
  comment?: string;
  metadata?: TaskMetadata;
  created_at: string;
  updated_at: string;
  created_by_id?: string;
  created_by_name?: string;
  updated_by_id?: string;
  updated_by_name?: string;

  static fromTask(task: Task): TaskDTO {
    const dto = new TaskDTO();
    dto.id = task.id;
    dto.action = task.action;
    dto.status = task.status;
    dto.open = task.open;
    dto.dataset_id = task.datasetId;
    dto.comment = task.comment ? task.comment : undefined;
    dto.metadata = task.metadata;
    dto.created_at = task.createdAt.toISOString();
    dto.updated_at = task.updatedAt.toISOString();
    dto.created_by_id = task.createdBy?.id;
    dto.created_by_name = task.createdBy?.name;
    dto.updated_by_id = task.updatedBy?.id;
    dto.updated_by_name = task.updatedBy?.name;

    return dto;
  }
}
