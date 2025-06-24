import { EventLog } from '../entities/event-log';

export class EventLogDTO {
  id: string;
  action: string;
  entity: string;
  entity_id: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  data?: Record<string, any>;
  user_id?: string;
  client?: string;
  created_at: Date;
  created_by?: string;

  static fromEventLog(eventLog: EventLog): EventLogDTO {
    const dto = new EventLogDTO();

    dto.id = eventLog.id;
    dto.action = eventLog.action;
    dto.entity = eventLog.entity;
    dto.entity_id = eventLog.entityId;
    dto.data = eventLog.data;
    dto.user_id = eventLog.userId;
    dto.client = eventLog.client;
    dto.created_at = eventLog.createdAt;
    dto.created_by = eventLog.user?.name;

    return dto;
  }
}
