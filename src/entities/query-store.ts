import { BaseEntity, Column, Entity, Index, PrimaryColumn } from 'typeorm';
import { ConsumerOptions } from '../interfaces/consumer-options';
import { FactTableToDimensionName } from '../interfaces/fact-table-column-to-dimension-name';

@Entity({ name: 'query_store' })
export class QueryStore extends BaseEntity {
  // nanoid and returned the user
  @PrimaryColumn({ type: 'text', primaryKeyConstraintName: 'PK_query_store_id' })
  id: string;

  // generated off the request object, so we don't end up
  // with duplicates.  When we generate an entry, we check the store
  // first for an existing match and give that ID back
  @Index('IDX_query_store_hash')
  @Column({ name: 'hash', type: 'text', nullable: false })
  hash: string;

  @Column({ name: 'dataset_id', type: 'uuid', nullable: false })
  datasetId: string;

  @Column({ name: 'revision_id', type: 'uuid', nullable: false })
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
