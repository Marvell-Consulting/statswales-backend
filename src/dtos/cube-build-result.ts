export interface CubeBuildResult {
  message: string;
  memory_usage: NodeJS.MemoryUsage;
  start_time: Date;
  finish_time: Date;
  total_time: number;
  error?: Error;
}
