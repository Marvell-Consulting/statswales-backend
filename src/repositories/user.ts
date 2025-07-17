import { appDataSource } from '../db/data-source';
import { RoleSelectionDTO } from '../dtos/user/role-selection-dto';
import { UserCreateDTO } from '../dtos/user/user-create-dto';
import { UserDTO } from '../dtos/user/user-dto';
import { User } from '../entities/user/user';
import { UserGroupRole } from '../entities/user/user-group-role';
import { AuthProvider } from '../enums/auth-providers';
import { GlobalRole } from '../enums/global-role';
import { GroupRole } from '../enums/group-role';
import { Locale } from '../enums/locale';
import { UserStatus } from '../enums/user-status';
import { ResultsetWithCount } from '../interfaces/resultset-with-count';

export const UserRepository = appDataSource.getRepository(User).extend({
  async getById(id: string): Promise<User> {
    return this.findOneOrFail({
      where: { id },
      relations: {
        groupRoles: {
          group: { metadata: true }
        }
      }
    });
  },

  async createUser(dto: UserCreateDTO, provider = AuthProvider.EntraId): Promise<User> {
    const email = dto.email.toLowerCase();
    const user = User.create({ email, provider });
    return user.save();
  },

  async listByLanguage(locale: Locale, page: number, limit: number): Promise<ResultsetWithCount<UserDTO>> {
    const lang = locale.includes('en') ? Locale.EnglishGb : Locale.WelshGb;

    const userQuery = this.find({
      relations: {
        groupRoles: {
          group: { metadata: true }
        }
      },
      order: { familyName: 'ASC', email: 'ASC' },
      skip: (page - 1) * limit,
      take: limit
    });

    const countQuery = this.createQueryBuilder('u');
    const [data, count] = await Promise.all([userQuery, countQuery.getCount()]);
    const userDtos = data.map((user) => UserDTO.fromUser(user, lang));

    return { data: userDtos, count };
  },

  async updateUserRoles(userId: string, roleSelection: RoleSelectionDTO[]): Promise<User> {
    await this.manager.transaction(async (transactionEm) => {
      const user = await transactionEm.findOneOrFail(User, {
        where: { id: userId },
        relations: { groupRoles: { group: { metadata: true } } }
      });

      // delete all existing group roles
      await transactionEm.getRepository(UserGroupRole).remove(user.groupRoles);
      user.groupRoles = [];

      // add the selected roles
      for (const selection of roleSelection) {
        if (selection.type === 'global') {
          user.globalRoles = selection.roles as GlobalRole[];
        } else if (selection.type === 'group' && selection.groupId) {
          const groupRole = UserGroupRole.create({ groupId: selection.groupId, roles: selection.roles as GroupRole[] });
          user.groupRoles.push(groupRole);
        }
      }

      return transactionEm.save(user);
    });

    return this.getById(userId);
  },

  async updateUserStatus(userId: string, status: UserStatus): Promise<User> {
    const user = await this.findOneOrFail({ where: { id: userId } });
    user.status = status;
    return this.save(user);
  }
});
