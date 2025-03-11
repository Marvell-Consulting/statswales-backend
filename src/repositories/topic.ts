import { dataSource } from '../db/data-source';
import { Topic } from '../entities/dataset/topic';

export const TopicRepository = dataSource.getRepository(Topic).extend({
  async listAll(): Promise<Topic[]> {
    return this.find();
  }
});
