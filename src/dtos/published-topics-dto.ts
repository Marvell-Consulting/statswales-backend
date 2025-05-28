import { ResultsetWithCount } from '../interfaces/resultset-with-count';
import { DatasetListItemDTO } from './dataset-list-item-dto';
import { TopicDTO } from './topic-dto';

export interface PublishedTopicsDTO {
  selectedTopic?: TopicDTO;
  children?: TopicDTO[];
  parents?: TopicDTO[];
  datasets?: ResultsetWithCount<DatasetListItemDTO>;
}
