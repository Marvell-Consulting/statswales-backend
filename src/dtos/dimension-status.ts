import { TaskStatus } from '../enums/task-status';

export interface DimensionStatus {
    name: string;
    status: TaskStatus;
}
