import { ILike } from 'typeorm';

import { publisherDataSource } from '../db/publisher-source';
import { Provider } from '../entities/dataset/provider';
import { Locale } from '../enums/locale';
import { ProviderSource } from '../entities/dataset/provider-source';

export const ProviderRepository = publisherDataSource.getRepository(Provider).extend({
  async listAllByLanguage(lang: Locale): Promise<Provider[]> {
    return this.find({
      where: { language: ILike(`${lang}%`) },
      order: { name: 'ASC' }
    });
  },

  async listAllSourcesByProvider(providerId: string, lang: Locale): Promise<ProviderSource[]> {
    return publisherDataSource.getRepository(ProviderSource).find({
      where: {
        providerId,
        language: ILike(`${lang}%`)
      },
      order: { name: 'ASC' }
    });
  }
});
