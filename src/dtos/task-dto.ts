import { Task } from '../entities/task/task';

export class TaskDTO {
  id: string;
  action: string;
  status: string;
  open: boolean;
  entity?: string;
  entity_id?: string;
  comment?: string;
  created_at: string;
  updated_at: string;
  submitted_by_id?: string;
  submitted_by_name?: string;
  response_by_id?: string;
  response_by_name?: string;

  static fromTask(task: Task): TaskDTO {
    const dto = new TaskDTO();
    dto.id = task.id;
    dto.action = task.action;
    dto.status = task.status;
    dto.open = task.open;
    dto.entity = task.entity;
    dto.entity_id = task.entityId;
    dto.comment = task.comment;
    dto.created_at = task.createdAt.toISOString();
    dto.updated_at = task.updatedAt.toISOString();
    dto.submitted_by_id = task.submittedBy?.id;
    dto.submitted_by_name = task.submittedBy?.name;
    dto.response_by_id = task.responseBy?.id;
    dto.response_by_name = task.responseBy?.name;

    return dto;
  }
}
