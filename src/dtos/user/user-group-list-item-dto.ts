export interface UserGroupListItemDTO {
  id: string;
  name: string;
  email: string;
  prefix?: string;
  user_count?: number;
  dataset_count?: number;
}
