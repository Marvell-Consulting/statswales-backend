import {
  EventSubscriber,
  EntitySubscriberInterface,
  DataSource,
  InsertEvent,
  UpdateEvent,
  RemoveEvent,
  DeepPartial,
  BaseEntity,
  ObjectLiteral
} from 'typeorm';
import { get, isArray, isObjectLike, isPlainObject, omitBy } from 'lodash';

import { logger } from '../utils/logger';
import { EventLog } from '../entities/event-log';
import { User } from '../entities/user/user';
import { asyncLocalStorage } from '../services/async-local-storage';

type AnyEntity = BaseEntity | ObjectLiteral;

type WriteEvent = InsertEvent<AnyEntity> | UpdateEvent<AnyEntity> | RemoveEvent<AnyEntity>;

// prevent logging of event_log table (infinite loop!) and anything else we want to ignore
const ignoreTables: string[] = ['event_log'];

// ignore some common props from the logged value that can be easily retrieved elsewhere
const ignoreProps: string[] = ['id', 'createdAt', 'updatedAt', 'createdBy'];

@EventSubscriber()
export class EntitySubscriber implements EntitySubscriberInterface {
  constructor(private dataSource: DataSource) {
    this.dataSource.subscribers.push(this);
    logger.debug('EntitySubscriber initialized');
  }

  private getUser(): User {
    return asyncLocalStorage.getStore()?.get('user');
  }

  private getClient(): string {
    // if there's a request id this came from the API, otherwise it's a system event
    // TODO: implement client ids so we can record multiple clients
    return asyncLocalStorage.getStore()?.get('requestId') ? 'sw3-frontend' : 'system';
  }

  private normaliseEntity(entity: AnyEntity): Record<string, unknown> {
    return omitBy(entity, (val: unknown, key: string) => {
      if (ignoreProps.includes(key)) return true;

      // ignore nested typeorm entities but include jsonb objects and arrays
      if (isObjectLike(val) && !isPlainObject(val) && !isArray(val)) return true;

      return false;
    });
  }

  private async logEvent(action: string, event: WriteEvent): Promise<void> {
    if (ignoreTables.includes(event?.metadata?.tableName)) return;

    try {
      const log: DeepPartial<EventLog> = {
        action,
        entity: event.metadata?.tableName,
        entityId: get(event, 'entityId') || get(event, 'entity.id'),
        data: event.entity ? this.normaliseEntity(event.entity) : undefined,
        userId: this.getUser()?.id,
        client: this.getClient()
      };

      await EventLog.save<EventLog>(log);
    } catch (err) {
      logger.error(err, 'failed to write to event log');
    }
  }

  async afterInsert(event: InsertEvent<AnyEntity>): Promise<void> {
    await this.logEvent('insert', event);
  }

  async afterUpdate(event: UpdateEvent<AnyEntity>): Promise<void> {
    if (!event.entity) {
      return;
    }
    await this.logEvent('update', event);
  }

  async afterRemove(event: RemoveEvent<AnyEntity>): Promise<void> {
    if (!event.entityId && !event.entity) {
      return;
    }
    await this.logEvent('delete', event);
  }
}
