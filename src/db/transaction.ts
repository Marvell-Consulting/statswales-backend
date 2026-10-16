import { EntityManager } from 'typeorm';

import { publisherDataSource } from './publisher-source';

// Runs `fn` inside a publisher DB transaction, committing when it resolves and rolling back if it throws.
//
// Only writes made through the supplied EntityManager take part in the transaction, so inside `fn`:
//   - use `em.withRepository(SomeRepository)` for custom repositories (their methods then run on `em`)
//   - use `em.getRepository(Entity)` or `em.save(...)` instead of active-record calls like `entity.save()`
//     or `Entity.getRepository()`, which always use the global connection and would escape the transaction
//
// Pass an existing transactional EntityManager as `em` to join its transaction (nested calls use a savepoint).
//
// Keep transactions short: never run cube builds, cube DB queries or file storage operations inside one, as the
// transaction holds a pooled connection (and any row locks) until it completes and none of those can be rolled back.
export function withTransaction<T>(
  fn: (em: EntityManager) => Promise<T>,
  em: EntityManager = publisherDataSource.manager
): Promise<T> {
  return em.transaction(fn);
}
