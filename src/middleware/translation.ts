/* eslint-disable import/no-named-as-default-member */
// this is the correct way to call i18next according to their docs
// linter was being whiny about not importing use

import i18next from 'i18next';
import Backend from 'i18next-fs-backend';
import * as i18nextMiddleware from 'i18next-http-middleware';

import { config } from '../config';

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
    debug: false,
    // Never echo an unrecognised key back to the caller verbatim — keys are sometimes
    // derived from raw error messages (e.g. DB/driver errors), so returning them as-is
    // would leak internal detail to API consumers. Return a fixed, generic string
    // instead. This is a hardcoded literal (not a nested t() call) so it can never
    // recurse back into this handler, regardless of locale/resource load state.
    parseMissingKeyHandler: () => 'An unknown error occurred'
  });

const t = i18next.t;

export { t, i18next, i18nextMiddleware, SUPPORTED_LOCALES, AVAILABLE_LANGUAGES };
