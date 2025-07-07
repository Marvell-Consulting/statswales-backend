import { i18next } from '../middleware/translation';
import { Error } from '../dtos/error';
import { Locale } from '../enums/locale';
import { MIN_PAGE_SIZE, MAX_PAGE_SIZE } from '../utils/page-defaults';

const t = i18next.t;

function validatePageSize(page_size: number): boolean {
  return page_size >= MIN_PAGE_SIZE && page_size <= MAX_PAGE_SIZE;
}

function validatePageNumber(page_number: number): boolean {
  return page_number >= 1;
}

function validatMaxPageNumber(page_number: number, max_page_number: number): boolean {
  return page_number <= max_page_number;
}

export function validateParams(page_number: number, max_page_number: number, page_size: number): Error[] {
  const errors: Error[] = [];
  if (!validatePageSize(page_size)) {
    errors.push({
      field: 'page_size',
      user_message: [
        {
          lang: Locale.English,
          message: t('errors.page_size', {
            lng: Locale.English,
            max_page_size: MAX_PAGE_SIZE,
            min_page_size: MIN_PAGE_SIZE
          })
        },
        {
          lang: Locale.Welsh,
          message: t('errors.page_size', {
            lng: Locale.Welsh,
            max_page_size: MAX_PAGE_SIZE,
            min_page_size: MIN_PAGE_SIZE
          })
        }
      ],
      message: {
        key: 'errors.page_size',
        params: { max_page_size: MAX_PAGE_SIZE, min_page_size: MIN_PAGE_SIZE }
      }
    });
  }
  if (!validatMaxPageNumber(page_number, max_page_number)) {
    errors.push({
      field: 'page_number',
      user_message: [
        {
          lang: Locale.English,
          message: t('errors.page_number_to_high', { lng: Locale.English, page_number: max_page_number })
        },
        {
          lang: Locale.Welsh,
          message: t('errors.page_number_to_high', { lng: Locale.Welsh, page_number: max_page_number })
        }
      ],
      message: {
        key: 'errors.page_number_to_high',
        params: { page_number: max_page_number }
      }
    });
  }
  if (!validatePageNumber(page_number)) {
    errors.push({
      field: 'page_number',
      user_message: [
        { lang: Locale.English, message: t('errors.page_number_to_low', { lng: Locale.English }) },
        { lang: Locale.Welsh, message: t('errors.page_number_to_low', { lng: Locale.Welsh }) }
      ],
      message: { key: 'errors.page_number_to_low', params: {} }
    });
  }
  return errors;
}
