export interface DatasetStats {
  incomplete: number;
  pending_approval: number;
  scheduled: number;
  published: number;
  action_requested: number;
  archived: number;
  offline: number;
  total: number;
}

export interface UserStats {
  active: number;
  inactive: number;
  last_7_days: number;
  total: number;
}

export interface DashboardStats {
  datasets?: DatasetStats;
  users?: UserStats;
}
