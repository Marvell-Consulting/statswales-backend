import { ILike } from 'typeorm';

import { appDataSource } from '../db/data-source';
import { Provider } from '../entities/dataset/provider';
import { Locale } from '../enums/locale';
import { ProviderSource } from '../entities/dataset/provider-source';

export const ProviderRepository = appDataSource.getRepository(Provider).extend({
  async listAllByLanguage(lang: Locale): Promise<Provider[]> {
    return this.find({ where: { language: ILike(`${lang}%`) } });
  },

  async listAllSourcesByProvider(providerId: string, lang: Locale): Promise<ProviderSource[]> {
    return appDataSource.getRepository(ProviderSource).find({
      where: {
        providerId,
        language: ILike(`${lang}%`)
      }
    });
  }
});
