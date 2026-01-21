import { dataSource } from '../db/data-source';
import { SearchLog } from '../entities/search-log';

export const SearchLogRepository = dataSource.getRepository(SearchLog).extend({
  getAll(): Promise<SearchLog[]> {
    return this.find();
  }
});
