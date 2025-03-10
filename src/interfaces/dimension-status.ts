import { TaskStatus } from '../enums/task-status';

export interface DimensionStatus {
  id: string;
  name: string;
  status: TaskStatus;
  type: string;
}
