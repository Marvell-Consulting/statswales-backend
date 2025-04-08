import { GroupRole } from '../../enums/group-role';
import { UserDTO } from './user-dto';

export interface UserWithRolesDTO {
  user: UserDTO;
  roles: GroupRole[];
}
