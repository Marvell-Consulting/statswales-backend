import { dataSource } from '../db/data-source';
import { UserGroupDTO } from '../dtos/user/user-group-dto';
import { UserGroupListItemDTO } from '../dtos/user/user-group-list-item-dto';
import { UserGroup } from '../entities/user/user-group';
import { UserGroupMetadata } from '../entities/user/user-group-metadata';
import { Locale } from '../enums/locale';
import { ResultsetWithCount } from '../interfaces/resultset-with-count';

export const UserGroupRepository = dataSource.getRepository(UserGroup).extend({
  async getById(id: string): Promise<UserGroup> {
    return this.findOneOrFail({
      where: { id },
      relations: { metadata: true, organisation: true }
    });
  },

  async updateGroup(group: UserGroup, dto: UserGroupDTO): Promise<UserGroup> {
    const updates = UserGroupDTO.toUserGroup(dto);
    return this.merge(group, updates).save();
  },

  async listByLanguage(locale: Locale, page: number, limit: number): Promise<ResultsetWithCount<UserGroupListItemDTO>> {
    const lang = locale.includes('en') ? Locale.EnglishGb : Locale.WelshGb;

    const qb = this.createQueryBuilder('ug')
      .select('ug.id', 'id')
      .addSelect('ug.prefix', 'prefix')
      .addSelect('ugm.name', 'name')
      .addSelect('COUNT(DISTINCT u.id)', 'user_count')
      .addSelect('COUNT(DISTINCT d.id)', 'dataset_count')
      .leftJoin('ug.metadata', 'ugm', 'ugm.language = :lang', { lang })
      .leftJoin('ug.users', 'u')
      .leftJoin('ug.datasets', 'd')
      .groupBy('ug.id, ugm.name, ug.prefix');

    const offset = (page - 1) * limit;
    const countQuery = qb.clone();
    const resultQuery = qb.orderBy('ugm.name', 'ASC').offset(offset).limit(limit);
    const [data, count] = await Promise.all([resultQuery.getRawMany(), countQuery.getCount()]);

    return { data, count };
  },

  async createGroup(name_en: string, name_cy: string): Promise<UserGroup> {
    return UserGroup.create({
      metadata: [
        UserGroupMetadata.create({ name: name_en, language: Locale.EnglishGb }),
        UserGroupMetadata.create({ name: name_cy, language: Locale.WelshGb })
      ]
    }).save();
  }
});
