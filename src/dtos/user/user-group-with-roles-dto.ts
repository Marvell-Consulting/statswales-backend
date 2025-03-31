import { UserRole } from '../../enums/user-role';
import { UserGroupDTO } from './user-group-dto';

export interface UserGroupWithRolesDTO {
  group: UserGroupDTO;
  roles: UserRole[];
}
