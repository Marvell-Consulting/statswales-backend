import { RevisionMetadata } from '../entities/dataset/revision-metadata';

export const translatableMetadataKeys: (keyof RevisionMetadata)[] = [
  'title',
  'summary',
  'collection',
  'quality',
  'roundingDescription',
  'reason'
] as const;

export type TranslatableMetadataKey = (typeof translatableMetadataKeys)[number];
