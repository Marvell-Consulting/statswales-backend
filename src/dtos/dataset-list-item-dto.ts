import { DatasetStatus } from '../enums/dataset-status';
import { PublishingStatus } from '../enums/publishing-status';

export interface DatasetListItemDTO {
  id: string;
  title: string;
  group_name?: string;
  code?: string;
  revision_by?: string;
  last_updated?: string;
  status?: DatasetStatus;
  publishing_status?: PublishingStatus;
  first_published_at?: string;
}
