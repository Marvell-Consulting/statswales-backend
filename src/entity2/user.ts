import { Entity, PrimaryGeneratedColumn, Column, BaseEntity } from 'typeorm';

@Entity()
export class User extends BaseEntity {
    @PrimaryGeneratedColumn('uuid')
    id: string;

    @Column({ unique: true })
    username: string;

    @Column({ unique: true })
    email: string;

    @Column({ nullable: true, unique: true })
    oidc_subject: string;

    @Column({ nullable: true })
    oidc_issuer: string;

    @Column({ type: 'text', nullable: true })
    access_token: string;

    @Column({ type: 'text', nullable: true })
    refresh_token: string;

    @Column({ type: 'text', nullable: true })
    id_token: string;

    @Column({ type: 'timestamp', nullable: true })
    token_expiry: Date;

    @Column({ nullable: true })
    first_name: string;

    @Column({ nullable: true })
    last_name: string;

    @Column({ nullable: true })
    profile_picture: string;

    @Column({ type: 'timestamp', default: () => 'CURRENT_TIMESTAMP' })
    created_at: Date;

    @Column({ type: 'timestamp', default: () => 'CURRENT_TIMESTAMP', onUpdate: 'CURRENT_TIMESTAMP' })
    updated_at: Date;

    @Column({ type: 'boolean', default: true })
    active: boolean;
}
