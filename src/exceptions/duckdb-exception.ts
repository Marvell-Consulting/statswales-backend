export interface DuckDBException {
  type: string;
  message: string;
  stack: string;
  errno: number;
  code: string;
  errorType: string;
}
