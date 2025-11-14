import { DataTableAction } from '../enums/data-table-action';

export interface ClientProcessedUpload {
  action: DataTableAction;
  original_file_name: string;
  data: never[];
  done: boolean;
}
