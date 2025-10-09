export type LargestDataset = { title: string; row_count: number; size_bytes?: number };
export type LongestDataset = { title: string; interval: string; status: string };

export interface DatasetStats {
  summary: {
    incomplete: number;
    pending_approval: number;
    scheduled: number;
    published: number;
    action_requested: number;
    archived: number;
    offline: number;
    total: number;
  };
  largest?: LargestDataset[];
  longest?: LongestDataset[];
}

export interface UserStats {
  active: number;
  published: number;
  total: number;
}

export type MostPublishedGroup = { name: string; count: number };

export interface UserGroupStats {
  most_published: MostPublishedGroup[];
}

export interface DashboardStats {
  datasets?: DatasetStats;
  users?: UserStats;
  groups?: UserGroupStats;
}
