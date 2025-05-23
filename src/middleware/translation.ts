import i18next from 'i18next';
import Backend from 'i18next-fs-backend';
import * as i18nextMiddleware from 'i18next-http-middleware';

import { appConfig } from '../config';

const config = appConfig();

const AVAILABLE_LANGUAGES = config.language.availableTranslations;
const SUPPORTED_LOCALES = config.language.supportedLocales;

i18next
  .use(Backend)
  .use(i18nextMiddleware.LanguageDetector)
  .init({
    detection: {
      order: ['querystring', 'header'],
      lookupQuerystring: 'lang',
      lookupHeader: 'accept-language',
      ignoreCase: true,
      caches: false
    },
    backend: {
      loadPath: `${__dirname}/../resources/locales/{{lng}}.json`
    },
    fallbackLng: config.language.fallback,
    preload: AVAILABLE_LANGUAGES,
    debug: false
  });

const t = i18next.t;

export { t, i18next, i18nextMiddleware, SUPPORTED_LOCALES, AVAILABLE_LANGUAGES };
