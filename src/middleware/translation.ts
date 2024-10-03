import i18next from 'i18next';
import Backend from 'i18next-fs-backend';
import i18nextMiddleware from 'i18next-http-middleware';

const ENGLISH = 'en';
const WELSH = 'cy';
const AVAILABLE_LANGUAGES = [WELSH, ENGLISH];

i18next
    .use(Backend)
    .use(i18nextMiddleware.LanguageDetector)
    .init({
        detection: {
            order: ['header'],
            lookupHeader: 'accept-language',
            ignoreCase: true,
            caches: false,
            ignoreRoutes: []
        },
        backend: {
            loadPath: `${__dirname}/../resources/locales/{{lng}}.json`
        },
        fallbackLng: ENGLISH,
        preload: AVAILABLE_LANGUAGES,
        debug: false
    });

const t = i18next.t;

export { t, i18next, i18nextMiddleware, ENGLISH, WELSH, AVAILABLE_LANGUAGES };
