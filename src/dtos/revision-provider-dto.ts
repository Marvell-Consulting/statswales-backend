import { IsISO8601, IsNotEmpty, IsOptional, IsString, IsUUID } from 'class-validator';
import { v4 as uuid } from 'uuid';

import { RevisionProvider } from '../entities/dataset/revision-provider';

export class RevisionProviderDTO {
  @IsUUID(4)
  @IsOptional()
  id?: string;

  @IsUUID(4)
  @IsOptional()
  group_id?: string;

  @IsUUID(4)
  @IsNotEmpty()
  revision_id: string;

  @IsString()
  @IsNotEmpty()
  language: string;

  @IsUUID(4)
  @IsNotEmpty()
  provider_id: string;

  provider_name?: string;

  @IsUUID(4)
  @IsOptional()
  source_id?: string;

  source_name?: string;

  @IsISO8601()
  @IsOptional()
  created_at?: string;

  static fromRevisionProvider(revProvider: RevisionProvider): RevisionProviderDTO {
    const dto = new RevisionProviderDTO();
    dto.id = revProvider.id;
    dto.group_id = revProvider.groupId;
    dto.revision_id = revProvider.revisionId;
    dto.language = revProvider.language;
    dto.provider_id = revProvider.providerId;
    dto.provider_name = revProvider.provider?.name;
    dto.source_id = revProvider.providerSourceId;
    dto.source_name = revProvider.providerSource?.name;
    dto.created_at = revProvider.createdAt.toISOString();

    return dto;
  }

  static toRevisionProvider(dto: RevisionProviderDTO): RevisionProvider {
    const revProvider = new RevisionProvider();
    revProvider.id = dto.id || uuid();
    revProvider.groupId = dto.group_id || uuid();
    revProvider.revisionId = dto.revision_id;
    revProvider.language = dto.language?.toLowerCase();
    revProvider.providerId = dto.provider_id;
    revProvider.providerSourceId = dto.source_id;
    revProvider.createdAt = dto.created_at ? new Date(dto.created_at) : new Date();

    return revProvider;
  }
}
