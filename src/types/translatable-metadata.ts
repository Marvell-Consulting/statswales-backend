import { DatasetMetadata } from '../entities/dataset/dataset-metadata';

export const translatableMetadataKeys: (keyof DatasetMetadata)[] = [
    'title',
    'description',
    'collection',
    'quality',
    'roundingDescription'
] as const;

export type TranslatableMetadataKey = (typeof translatableMetadataKeys)[number];
