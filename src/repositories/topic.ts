import { In } from 'typeorm';
import { dataSource } from '../db/data-source';
import { Topic } from '../entities/dataset/topic';

export const TopicRepository = dataSource.getRepository(Topic).extend({
  async listAll(): Promise<Topic[]> {
    return this.find();
  },

  async getParents(path: string): Promise<Topic[]> {
    const parentIds = path.split('.');

    return this.find({
      where: { id: In(parentIds) }
    });
  }
});
