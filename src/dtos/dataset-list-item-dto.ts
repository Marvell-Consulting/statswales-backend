import { DatasetStatus } from '../enums/dataset-status';
import { PublishingStatus } from '../enums/publishing-status';

export interface DatasetListItemDTO {
  id: string;
  title: string;
  group_name?: string;
  code?: string;
  revision_by?: string;
  status?: DatasetStatus;
  publishing_status?: PublishingStatus;
  first_published_at?: string;
  unpublished_at?: string;
  last_updated_at?: string;
  archived_at?: string;
}
