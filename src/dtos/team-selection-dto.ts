import { IsUUID } from 'class-validator';

export class TeamSelectionDTO {
  @IsUUID(4)
  team_id: string;
}
