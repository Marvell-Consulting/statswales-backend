import { dataSource } from '../db/data-source';
import { UserCreateDTO } from '../dtos/user/user-create-dto';
import { UserDTO } from '../dtos/user/user-dto';
import { User } from '../entities/user/user';
import { Locale } from '../enums/locale';
import { ResultsetWithCount } from '../interfaces/resultset-with-count';

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

  async createUser(dto: UserCreateDTO): Promise<User> {
    const user = User.create({ ...dto });
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
      order: { familyName: 'ASC' },
      skip: (page - 1) * limit,
      take: limit
    });

    const countQuery = this.createQueryBuilder('u');
    const [data, count] = await Promise.all([userQuery, countQuery.getCount()]);
    const userDtos = data.map((user) => UserDTO.fromUser(user, lang));

    return { data: userDtos, count };
  }
});
