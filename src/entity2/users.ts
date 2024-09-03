import { Entity, PrimaryGeneratedColumn, Column, BaseEntity } from 'typeorm';

@Entity()
export class Users extends BaseEntity {
    @PrimaryGeneratedColumn('uuid')
    id: string;

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

    @Column({ type: process.env.NODE_ENV === 'test' ? 'datetime' : 'timestamp', nullable: true })
    token_expiry: Date;

    @Column({ nullable: true })
    name: string;

    @Column({ nullable: true })
    given_name: string;

    @Column({ nullable: true })
    last_name: string;

    @Column({ nullable: true })
    profile_picture: string;

    @Column({ type: process.env.NODE_ENV === 'test' ? 'datetime' : 'timestamp', default: () => 'CURRENT_TIMESTAMP' })
    created_at: Date;

    @Column({
        type: process.env.NODE_ENV === 'test' ? 'datetime' : 'timestamp',
        default: () => 'CURRENT_TIMESTAMP',
        onUpdate: 'CURRENT_TIMESTAMP'
    })
    updated_at: Date;

    @Column({ type: 'boolean', default: true })
    active: boolean;

    public static getTestUser(): Users {
        const user = new Users();
        user.id = '12345678-1234-1234-1234-123456789012';
        user.email = 'test@test.com';
        user.oidc_subject = '';
        user.oidc_issuer = 'localAuth';
        user.access_token = '';
        user.refresh_token = '';
        user.id_token = '';
        user.token_expiry = new Date();
        user.name = 'Test User';
        user.given_name = 'Test';
        user.last_name = 'User';
        user.profile_picture = '';
        user.created_at = new Date();
        user.updated_at = new Date();
        user.active = true;
        return user;
    }
}
