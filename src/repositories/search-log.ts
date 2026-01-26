import { Between } from 'typeorm';
import { dataSource } from '../db/data-source';
import { SearchLog } from '../entities/search-log';

export const SearchLogRepository = dataSource.getRepository(SearchLog).extend({
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
