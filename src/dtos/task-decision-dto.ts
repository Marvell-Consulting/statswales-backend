import { IsEnum, IsOptional, IsString } from 'class-validator';

export class TaskDecisionDTO {
  @IsEnum(['approve', 'reject'])
  decision?: 'approve' | 'reject' | undefined;

  @IsString()
  @IsOptional()
  reason?: string;
}
