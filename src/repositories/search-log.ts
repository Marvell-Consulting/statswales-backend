import { Between } from 'typeorm';
import { consumerDataSource } from '../db/consumer-source';
import { SearchLog } from '../entities/search-log';

export const SearchLogRepository = consumerDataSource.getRepository(SearchLog).extend({
  getByPeriod(start: Date, end: Date): Promise<SearchLog[]> {
    return this.find({
      where: {
        createdAt: Between(start, end)
      },
      order: {
        createdAt: 'ASC'
      }
    });
  }
});
