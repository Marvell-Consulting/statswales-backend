import { IsEmail, IsEnum, IsOptional, IsString, IsUUID } from 'class-validator';
import { DeepPartial } from 'typeorm';

import { UserGroupMetadata } from '../../entities/user/user-group-metadata';
import { Locale } from '../../enums/locale';

export class UserGroupMetadataDTO {
  @IsUUID(4)
  @IsOptional()
  id?: string;

  @IsString()
  @IsOptional()
  name?: string;

  @IsEmail()
  @IsOptional()
  email?: string;

  @IsEnum(Locale)
  language?: string;

  static fromUserGroupMetadata(meta: UserGroupMetadata) {
    const dto = new UserGroupMetadataDTO();
    dto.id = meta.id;
    dto.name = meta.name;
    dto.email = meta.email?.toLowerCase();
    dto.language = meta.language;
    return dto;
  }

  static toUserGroupMetadata(dto: UserGroupMetadataDTO): DeepPartial<UserGroupMetadata> {
    return UserGroupMetadata.create({
      id: dto.id,
      name: dto.name,
      email: dto.email,
      language: dto.language
    });
  }
}
