import { RelatedLink } from '../dtos/related-link-dto';
import { TranslationDTO } from '../dtos/translations-dto';
import { Dataset } from '../entities/dataset/dataset';
import { translatableMetadataKeys } from '../types/translatable-metadata';
import { pick } from 'lodash';

export const collectTranslations = (dataset: Dataset, includeIds = false): TranslationDTO[] => {
  const revision = dataset.draftRevision!;
  const metadataEN = revision.metadata?.find((meta) => meta.language.includes('en'));
  const metadataCY = revision.metadata?.find((meta) => meta.language.includes('cy'));

  // ignore roundingDescription if rounding isn't applied
  const metadataKeys = translatableMetadataKeys.filter((key) => {
    return revision.roundingApplied === true ? true : key !== 'roundingDescription';
  });

  const translations: TranslationDTO[] = [
    ...(dataset.dimensions || []).map((dimension) => {
      const factTableColumn = dimension.factTableColumn;
      const dimMetaEN = dimension.metadata?.find((meta) => meta.language.includes('en'));
      const dimMetaCY = dimension.metadata?.find((meta) => meta.language.includes('cy'));
      const dimNameEN = dimMetaEN?.name === factTableColumn ? '' : dimMetaEN?.name;
      const dimNameCY = dimMetaCY?.name === factTableColumn ? '' : dimMetaCY?.name;

      return {
        type: 'dimension',
        key: dimension.factTableColumn,
        english: dimNameEN,
        cymraeg: dimNameCY,
        id: dimension.id
      };
    }),
    ...metadataKeys.map((prop) => ({
      type: 'metadata',
      key: prop,
      english: metadataEN?.[prop] as string,
      cymraeg: metadataCY?.[prop] as string
    })),
    ...(revision.relatedLinks || []).map((link: RelatedLink) => ({
      type: 'link',
      key: link.id,
      english: link.labelEN,
      cymraeg: link.labelCY
    }))
  ];

  return includeIds ? translations : translations.map((row) => pick(row, ['type', 'key', 'english', 'cymraeg']));
};
