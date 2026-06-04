import { In } from 'typeorm';
import { publisherDataSource } from '../db/publisher-source';
import { Topic } from '../entities/dataset/topic';

export const TopicRepository = publisherDataSource.getRepository(Topic).extend({
  async listAll(): Promise<Topic[]> {
    return this.find();
  },

  async getParents(path: string): Promise<Topic[]> {
    const parentIds = path.split('.');
    parentIds.pop(); // the last element is the child, remove it

    return this.find({
      where: { id: In(parentIds) }
    });
  }
});
