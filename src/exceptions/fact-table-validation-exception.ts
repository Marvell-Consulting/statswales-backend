import { FactTableValidationExceptionType } from '../enums/fact-table-validation-exception-type';
import { ColumnHeader } from '../dtos/view-dto';

export class FactTableValidationException implements Error {
  constructor(message: string, type: FactTableValidationExceptionType, status?: number) {
    this.message = message;
    this.tag = `errors.fact_table_validation.${type}`;
    this.type = type;
    this.status = status || 400;
  }
  status: number;
  message: string;
  type: FactTableValidationExceptionType;
  tag: string;
  name: string;
  data: string[][];
  headers: ColumnHeader[];
}
