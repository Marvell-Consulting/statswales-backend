import { FindManyOptions, ILike, IsNull, Not } from 'typeorm';

import { dataSource } from '../db/data-source';
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
import { UserStats } from '../interfaces/dashboard-stats';

export const UserRepository = dataSource.getRepository(User).extend({
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

  async listByLanguage(
    locale: Locale,
    page: number,
    limit: number,
    search?: string
  ): Promise<ResultsetWithCount<UserDTO>> {
    const lang = locale.includes('en') ? Locale.EnglishGb : Locale.WelshGb;
    const countQuery = this.createQueryBuilder('u');

    const findOpts: FindManyOptions = {
      relations: {
        groupRoles: {
          group: { metadata: true }
        }
      },
      order: { name: 'ASC', email: 'ASC' },
      skip: (page - 1) * limit,
      take: limit
    };

    if (search) {
      findOpts.where = [{ email: ILike(`%${search}%`) }, { name: ILike(`%${search}%`) }];
      countQuery.where('u.email ILIKE :search OR u.name ILIKE :search', { search: `%${search}%` });
    }

    const userQuery = this.find(findOpts);
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
  },

  async getDashboardStats(): Promise<UserStats> {
    const activeQuery = this.count({ where: { status: UserStatus.Active, lastLoginAt: Not(IsNull()) } });

    const publishedQuery = this.count({
      relations: { datasets: true },
      where: { datasets: { firstPublishedAt: Not(IsNull()) } }
    });

    const totalQuery = this.count();

    const mostPublishedQuery = this.query(`
      SELECT u.id AS id, name, COUNT(d.id) AS count
      FROM "user" u
      INNER JOIN dataset d ON d.created_by = u.id
      WHERE d.first_published_at IS NOT NULL
      AND d.first_published_at <= NOW()
      GROUP BY u.id
      ORDER BY count DESC, name ASC
      LIMIT 10
    `);

    const [active, published, total, most_published] = await Promise.all([
      activeQuery,
      publishedQuery,
      totalQuery,
      mostPublishedQuery
    ]);

    const summary = { active, published, total };

    return { summary, most_published };
  }
});
