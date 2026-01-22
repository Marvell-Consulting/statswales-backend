import { BaseEntity, Column, CreateDateColumn, Entity, PrimaryGeneratedColumn } from 'typeorm';
import { SearchMode } from '../enums/search-mode';

@Entity({ name: 'search_log' })
export class SearchLog extends BaseEntity {
  @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_search_log_id' })
  id: string;

  @Column({ name: 'mode', type: 'text', nullable: false })
  mode: SearchMode;

  @Column({ name: 'keywords', type: 'text', nullable: false })
  keywords: string;

  @Column({ name: 'result_count', type: 'integer', nullable: true })
  resultCount?: number;

  @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
  createdAt: Date;
}
