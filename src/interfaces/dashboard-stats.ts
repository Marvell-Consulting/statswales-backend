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
  total: number;
  active: number;
  last_7_days: number;
}

export interface DashboardStats {
  datasets?: DatasetStats;
  users?: UserStats;
}
