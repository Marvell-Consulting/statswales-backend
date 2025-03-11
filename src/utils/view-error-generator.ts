import { t } from 'i18next';

import { ViewErrDTO } from '../dtos/view-dto';
import { ErrorMessage } from '../dtos/error';
import { AVAILABLE_LANGUAGES } from '../middleware/translation';

export function viewErrorGenerator(
  status: number,
  dataset_id: string,
  field: string,
  tag: string,
  extension: object
): ViewErrDTO {
  const messages: ErrorMessage[] = AVAILABLE_LANGUAGES.map((lang) => {
    return {
      message: t(tag, { lng: lang }),
      lang
    };
  });
  return {
    status,
    dataset_id,
    errors: [
      {
        field,
        tag: { name: tag, params: {} },
        message: messages
      }
    ],
    extension
  };
}
