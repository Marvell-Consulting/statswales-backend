import { Entity, PrimaryGeneratedColumn, Column, BaseEntity } from 'typeorm';

@Entity({ name: 'users' })
export class User extends BaseEntity {
    @PrimaryGeneratedColumn('uuid')
    id: string;

    @Column({ unique: true })
    email: string;

    @Column({ nullable: true })
    name: string;

    @Column({ name: 'given_name', nullable: true })
    givenName: string;

    @Column({ name: 'last_name', nullable: true })
    lastName: string;

    @Column({
        name: 'created_at',
        type: process.env.NODE_ENV === 'test' ? 'datetime' : 'timestamptz',
        default: () => 'CURRENT_TIMESTAMP'
    })
    createdAt: Date;

    @Column({
        name: 'updated_at',
        type: process.env.NODE_ENV === 'test' ? 'datetime' : 'timestamptz',
        default: () => 'CURRENT_TIMESTAMP',
        onUpdate: 'CURRENT_TIMESTAMP'
    })
    updatedAt: Date;

    @Column({ type: 'boolean', default: true })
    active: boolean;

    public static getTestUser(): User {
        const user = new User();
        user.id = '12345678-1234-1234-1234-123456789012';
        user.email = 'test@test.com';
        user.name = 'Test User';
        user.givenName = 'Test';
        user.lastName = 'User';
        user.createdAt = new Date();
        user.updatedAt = new Date();
        user.active = true;
        return user;
    }
}
