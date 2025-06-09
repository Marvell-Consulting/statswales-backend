import { IsEnum, IsOptional, IsUUID } from 'class-validator';

import { GlobalRole } from '../../enums/global-role';
import { GroupRole } from '../../enums/group-role';

export class RoleSelectionDTO {
  @IsEnum(['global', 'group'])
  type: 'global' | 'group';

  @IsEnum([...Object.values(GlobalRole), ...Object.values(GroupRole)], { each: true })
  roles: GlobalRole[] | GroupRole[];

  @IsUUID(4)
  @IsOptional()
  groupId?: string;
}
