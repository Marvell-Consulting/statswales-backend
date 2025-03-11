import { Provider } from '../entities/dataset/provider';

export class ProviderDTO {
  id: string;
  language: string;
  name: string;

  static fromProvider(provider: Provider): ProviderDTO {
    const dto = new ProviderDTO();
    dto.id = provider.id;
    dto.language = provider.language;
    dto.name = provider.name;

    return dto;
  }
}
