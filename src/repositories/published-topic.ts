import { In } from 'typeorm';
import { consumerDataSource } from '../db/consumer-source';
import { Topic } from '../entities/dataset/topic';

/**
 * Read-only Topic repo bound to the consumer pool. Mirrors the published-dataset / published-revision
 * pattern: consumer routes use this so they don't borrow connections from the publisher pool.
 * Topic writes (e.g. publisher /topic routes, seeders) continue to go via TopicRepository.
 */
export const PublishedTopicRepository = consumerDataSource.getRepository(Topic).extend({
  async getParents(path: string): Promise<Topic[]> {
    const parentIds = path.split('.');
    parentIds.pop(); // last element is the child

    return this.find({
      where: { id: In(parentIds) }
    });
  }
});
