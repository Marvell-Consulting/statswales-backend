import { dataSource } from '../db/data-source';
import { UserGroupDTO } from '../dtos/user/user-group-dto';
import { UserGroupListItemDTO } from '../dtos/user/user-group-list-item-dto';
import { UserGroupMetadataDTO } from '../dtos/user/user-group-metadata-dto';
import { UserGroup } from '../entities/user/user-group';
import { Locale } from '../enums/locale';
import { ResultsetWithCount } from '../interfaces/resultset-with-count';

export const UserGroupRepository = dataSource.getRepository(UserGroup).extend({
  async getById(id: string): Promise<UserGroup> {
    return this.findOneOrFail({
      where: { id },
      relations: {
        metadata: true,
        organisation: { metadata: true },
        datasets: { endRevision: { metadata: true } }
      }
    });
  },

  async getAll(): Promise<UserGroup[]> {
    return this.find({
      relations: {
        metadata: true,
        organisation: { metadata: true }
      }
    });
  },

  async listByLanguage(locale: Locale, page: number, limit: number): Promise<ResultsetWithCount<UserGroupListItemDTO>> {
    const lang = locale.includes('en') ? Locale.EnglishGb : Locale.WelshGb;

    const qb = this.createQueryBuilder('ug')
      .select(['ug.id AS id', 'ug.prefix AS prefix', 'ugm.name AS name', 'ugm.email AS email'])
      .addSelect('COUNT(DISTINCT u.id)', 'user_count')
      .addSelect('COUNT(DISTINCT d.id)', 'dataset_count')
      .leftJoin('ug.metadata', 'ugm', 'ugm.language = :lang', { lang })
      .leftJoin('ug.datasets', 'd')
      .leftJoin('ug.groupRoles', 'ugr')
      .leftJoin('ugr.user', 'u')
      .groupBy('ug.id, ugm.name, ugm.email, ug.prefix');

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

  async updateGroup(group: UserGroup, dto: UserGroupDTO): Promise<UserGroup> {
    // reload group without relations to allow merge to work correctly
    group = await this.findOneByOrFail({ id: group.id });
    const updates = UserGroupDTO.toUserGroup(dto);
    return this.merge(group, updates).save();
  }
});
