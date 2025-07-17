import { In } from 'typeorm';
import { appDataSource } from '../db/data-source';
import { Topic } from '../entities/dataset/topic';

export const TopicRepository = appDataSource.getRepository(Topic).extend({
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
