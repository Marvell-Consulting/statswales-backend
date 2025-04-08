import { GroupRole } from '../../enums/group-role';
import { UserGroupDTO } from './user-group-dto';

export interface UserGroupWithRolesDTO {
  group: UserGroupDTO;
  roles: GroupRole[];
}
