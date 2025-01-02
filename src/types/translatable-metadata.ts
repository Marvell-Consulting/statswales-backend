import { DatasetInfo } from '../entities/dataset/dataset-info';

export const translatableMetadataKeys: (keyof DatasetInfo)[] = [
    'title',
    'description',
    'collection',
    'quality',
    'roundingDescription'
] as const;

export type TranslatableMetadataKey = (typeof translatableMetadataKeys)[number];
