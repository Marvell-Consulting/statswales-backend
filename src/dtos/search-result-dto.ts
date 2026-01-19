export interface SearchResultDTO {
  id: string;
  title: string;
  summary?: string;
  match?: string;
  rank?: string;
  first_published_at?: string;
  unpublished_at?: string;
  last_updated_at?: string;
  archived_at?: string;
}
