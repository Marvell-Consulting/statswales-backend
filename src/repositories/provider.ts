import { ILike } from 'typeorm';

import { dataSource } from '../db/data-source';
import { Provider } from '../entities/dataset/provider';
import { Locale } from '../enums/locale';
import { ProviderSource } from '../entities/dataset/provider-source';

export const ProviderRepository = dataSource.getRepository(Provider).extend({
    async listAllByLanguage(lang: Locale): Promise<Provider[]> {
        return this.find({ where: { language: ILike(`${lang}%`) } });
    },

    async listAllSourcesByProvider(providerId: string, lang: Locale): Promise<ProviderSource[]> {
        return dataSource.getRepository(ProviderSource).find({
            where: {
                providerId,
                language: ILike(`${lang}%`)
            }
        });
    }
});
