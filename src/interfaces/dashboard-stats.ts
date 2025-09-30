export interface DatasetStats {
  incomplete: number;
  pendingApproval: number;
  published: number;
  archived: number;
  offline: number;
  total: number;
}

export interface UserStats {
  total: number;
  active: number;
  last7Days: number;
}

export interface DashboardStats {
  datasets?: DatasetStats;
  users?: UserStats;
}
