import { DataTable } from '../entities/dataset/data-table';
import { DataTableDescription } from '../entities/dataset/data-table-description';
import { DataTableAction } from '../enums/data-table-action';

import { DataTableDescriptionDto } from './data-table-description-dto';

export class DataTableDto {
  id: string;
  mime_type: string;
  filename: string;
  original_filename: string;
  file_type: string;
  hash: string;
  uploaded_at?: string;
  revision_id?: string;
  descriptors: DataTableDescriptionDto[];
  action?: DataTableAction;

  static fromDataTable(dataTable: DataTable): DataTableDto {
    const dto = new DataTableDto();
    dto.id = dataTable.id;
    dto.mime_type = dataTable.mimeType;
    dto.filename = dataTable.filename;
    dto.original_filename = dataTable.originalFilename;
    dto.file_type = dataTable.fileType;
    dto.hash = dataTable.hash;
    dto.uploaded_at = dataTable.uploadedAt?.toISOString();
    dto.descriptors = [];
    dto.revision_id = dataTable.revision?.id;
    dto.action = dataTable.action;

    dto.descriptors = dataTable.dataTableDescriptions?.map((factTableInfo: DataTableDescription) =>
      DataTableDescriptionDto.fromDataTableDescription(factTableInfo)
    );
    return dto;
  }
}
