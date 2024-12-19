export enum Locale {
    English = 'en',
    Welsh = 'cy',
    EnglishGb = 'en-GB',
    WelshGb = 'cy-GB'
}

export const SupportedLanguagues = Object.freeze({
    English: { code: Locale.English, name: 'English', locale: Locale.EnglishGb },
    Welsh: { code: Locale.Welsh, name: 'Welsh', locale: Locale.WelshGb }
});
