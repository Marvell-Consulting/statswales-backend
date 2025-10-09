import { FindManyOptions } from 'typeorm';
import { dataSource } from '../db/data-source';
import { UserGroupDTO } from '../dtos/user/user-group-dto';
import { UserGroupListItemDTO } from '../dtos/user/user-group-list-item-dto';
import { UserGroupMetadataDTO } from '../dtos/user/user-group-metadata-dto';
import { UserGroup } from '../entities/user/user-group';
import { UserGroupRole } from '../entities/user/user-group-role';
import { Locale } from '../enums/locale';
import { UserGroupStatus } from '../enums/user-group-status';
import { ResultsetWithCount } from '../interfaces/resultset-with-count';
import { UserGroupStats } from '../interfaces/dashboard-stats';

export const UserGroupRepository = dataSource.getRepository(UserGroup).extend({
  async getById(id: string): Promise<UserGroup> {
    return this.findOneByOrFail({ id });
  },

  async getByIdWithOrganisation(id: string): Promise<UserGroup> {
    return this.findOneOrFail({
      where: { id },
      relations: {
        metadata: true,
        organisation: { metadata: true }
      }
    });
  },

  async getByIdWithDatasets(id: string): Promise<UserGroup> {
    return this.findOneOrFail({
      where: { id },
      relations: {
        metadata: true,
        organisation: { metadata: true },
        datasets: { endRevision: { metadata: true } },
        groupRoles: { user: true }
      }
    });
  },

  async getAll(status?: UserGroupStatus): Promise<UserGroup[]> {
    const findOptions: FindManyOptions<UserGroup> = {
      relations: {
        metadata: true,
        organisation: { metadata: true }
      },
      order: {
        metadata: { name: 'ASC' }
      }
    };

    if (status) {
      findOptions.where = { status };
    }

    return this.find(findOptions);
  },

  async listByLanguage(
    locale: Locale,
    page: number,
    limit: number,
    search?: string
  ): Promise<ResultsetWithCount<UserGroupListItemDTO>> {
    const lang = locale.includes('en') ? Locale.EnglishGb : Locale.WelshGb;

    const qb = this.createQueryBuilder('ug')
      .select([
        'ug.id AS id',
        'ug.prefix AS prefix',
        'ugm.name AS name',
        'ugm.email AS email',
        'ug.status AS status',
        'om.name AS organisation'
      ])
      .addSelect('COUNT(DISTINCT u.id)', 'user_count')
      .addSelect('COUNT(DISTINCT d.id)', 'dataset_count')
      .leftJoin('ug.metadata', 'ugm', 'ugm.language = :lang', { lang })
      .leftJoin('ug.datasets', 'd')
      .leftJoin('ug.groupRoles', 'ugr')
      .leftJoin('ugr.user', 'u')
      .leftJoin('ug.organisation', 'o')
      .leftJoin('o.metadata', 'om', 'om.language = :lang', { lang })
      .groupBy('ug.id, ugm.name, ugm.email, ug.prefix, ug.status, om.name')
      .orderBy('ugm.name', 'ASC');

    if (search) {
      qb.where('ugm.name ILIKE :search OR ugm.email ILIKE :search', { search: `%${search}%` });
    }

    const offset = (page - 1) * limit;
    const countQuery = qb.clone();
    const resultQuery = qb.orderBy('ugm.name', 'ASC').offset(offset).limit(limit);
    const [data, count] = await Promise.all([resultQuery.getRawMany(), countQuery.getCount()]);

    return { data, count };
  },

  async createGroup(meta: UserGroupMetadataDTO[]): Promise<UserGroup> {
    const metadata = meta.map((m) => UserGroupMetadataDTO.toUserGroupMetadata(m));
    return UserGroup.create({ metadata }).save();
  },

  async updateGroup(groupId: string, dto: UserGroupDTO): Promise<UserGroup> {
    const group = await this.findOneByOrFail({ id: groupId });
    const updates = UserGroupDTO.toUserGroup(dto);
    return this.merge(group, updates).save();
  },

  async updateGroupStatus(groupId: string, status: UserGroupStatus): Promise<UserGroup> {
    const group = await this.findOneOrFail({
      where: { id: groupId },
      relations: { groupRoles: true }
    });

    // delete any associated user roles when deactivating
    if (status === UserGroupStatus.Inactive && group.groupRoles && group.groupRoles.length > 0) {
      await dataSource.getRepository(UserGroupRole).delete({ group: { id: groupId } });
    }

    group.status = status;
    await this.save(group);

    return this.findOneByOrFail({ id: groupId });
  },

  async getDashboardStats(): Promise<UserGroupStats> {
    const most_published = await this.createQueryBuilder('ug')
      .select('ugm.name', 'name')
      .addSelect('COUNT(d.id)', 'count')
      .leftJoin('ug.datasets', 'd', 'd.firstPublishedAt IS NOT NULL')
      .leftJoin('ug.metadata', 'ugm', 'ugm.language = :lang', { lang: Locale.EnglishGb })
      .groupBy('ug.id, ugm.name')
      .orderBy('count', 'DESC')
      .having('COUNT(d.id) > 0')
      .limit(5)
      .getRawMany();

    return { most_published };
  }
});
