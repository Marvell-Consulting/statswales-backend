import { BaseEntity, Column, Entity, PrimaryColumn } from 'typeorm';
import { ConsumerOptions } from '../interfaces/consumer-options';
import { FactTableToDimensionName } from '../interfaces/fact-table-column-to-dimension-name';

@Entity({ name: 'query-store' })
export class QueryStore extends BaseEntity {
  // nanoid and returned the user
  @PrimaryColumn({ type: 'text' })
  id: string;

  // generated off the request object, so we don't end up
  // with duplicates.  When we generate an entry, we check the store
  // first for an existing match and give that ID back
  @Column({ name: 'hash', type: 'text', nullable: false })
  hash: string;

  @Column({ name: 'dataset_id', type: 'uuid' })
  datasetId: string;

  @Column({ name: 'revision_id', type: 'uuid' })
  revisionId: string;

  // The request object from the post request, used to regenerate the entry
  @Column({ name: 'request_object', type: 'jsonb', nullable: false })
  requestObject: ConsumerOptions;

  // The resulting query for quicker playback
  @Column({ name: 'query', type: 'jsonb', nullable: false })
  query: Record<string, string>;

  @Column({ name: 'total_lines', type: 'int', nullable: false })
  totalLines: number;

  @Column({ name: 'column_mapping', type: 'jsonb', nullable: false })
  columnMapping: FactTableToDimensionName[];
}
