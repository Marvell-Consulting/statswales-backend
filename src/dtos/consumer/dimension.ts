import { DimensionMetadataDTO } from '../dimension-metadata-dto';

export interface Dimension {
  id: string;
  factTableColumn: string; // <-- Tells you which column in the fact table you're joining to
  metadata?: DimensionMetadataDTO;
}
