import { BaseEntity, Column, Entity, PrimaryColumn } from 'typeorm';
import { ConsumerRequestBody } from '../dtos/consumer/consumer-request-body';

@Entity({ name: 'query-store' })
export class QueryStore extends BaseEntity {
  // nanoid and returned the user
  @PrimaryColumn({ type: 'string' })
  id: string;

  // generated off the query or request object, so we don't end up
  // with duplicates.  When we generate an entry, we check the store
  // first for an existing match and give that ID back
  @Column({ name: 'hash', type: 'text', nullable: false })
  hash: string;

  @Column({ name: 'dataset_id' })
  datasetId: string;

  @Column({ name: 'revision_id' })
  revisionId: string;

  // The request object from the post request
  @Column({ name: 'request_object', type: 'jsonb', nullable: false })
  requestObject: ConsumerRequestBody;

  // The resulting query for quicker play back
  @Column({ name: 'query', type: 'text', nullable: false })
  query: string;
}
