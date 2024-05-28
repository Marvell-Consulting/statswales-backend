/* eslint-disable import/no-cycle */
import { UUID } from 'crypto';

import { Entity, PrimaryGeneratedColumn, Column, CreateDateColumn, BaseEntity, OneToMany, JoinColumn } from 'typeorm';

import { dbManager } from '../app';

import { LookupTable } from './lookuptable';
import { Datafile } from './datafile';
import { DatasetDescription } from './dataset_description';
import { DatasetTitle } from './dataset_title';
import { DatasetColumn } from './dataset_column';

@Entity({ name: 'datasets' })
export class Dataset extends BaseEntity {
    @PrimaryGeneratedColumn('uuid')
    id: string;

    @Column({ name: 'internal_name', nullable: false })
    internalName: string;

    @CreateDateColumn({ name: 'creation_date' })
    creationDate: Date;

    @Column({ name: 'created_by', nullable: true })
    createdBy: string;

    @CreateDateColumn({ name: 'last_modified' })
    lastModified: Date;

    @Column({ name: 'modified_by', nullable: true })
    modifiedBy: string;

    @Column({ name: 'publish_date', nullable: true })
    publishData: Date;

    @Column({ name: 'published_by', nullable: true })
    publishedBy: string;

    @Column({ nullable: true })
    live: boolean;

    @Column({ nullable: true })
    code: string;

    @OneToMany(() => Datafile, (datafile) => datafile.dataset, {
        cascade: true,
        orphanedRowAction: 'delete'
    })
    @JoinColumn()
    datafiles: Promise<Datafile[]>;

    @OneToMany(() => LookupTable, (lookupTable) => lookupTable.dataset, {
        cascade: true,
        orphanedRowAction: 'delete'
    })
    @JoinColumn()
    lookuptables: Promise<LookupTable[]>;

    @OneToMany(() => DatasetTitle, (datasetTitle) => datasetTitle.dataset, {
        cascade: true,
        orphanedRowAction: 'delete'
    })
    @JoinColumn([{ referencedColumnName: 'dataset_id' }, { referencedColumnName: 'language' }])
    title: Promise<DatasetTitle[]>;

    @OneToMany(() => DatasetDescription, (datasetDescription) => datasetDescription.dataset, {
        cascade: true,
        orphanedRowAction: 'delete'
    })
    @JoinColumn([{ referencedColumnName: 'dataset_id' }, { referencedColumnName: 'language' }])
    description: Promise<DatasetDescription[]>;

    @OneToMany(() => DatasetColumn, (datasetColumn) => datasetColumn.dataset, {
        cascade: true,
        orphanedRowAction: 'delete'
    })
    @JoinColumn()
    columns: Promise<DatasetColumn[]>;

    public static createDataset(internalName: string, user?: string, id?: UUID): Dataset {
        const dataset = new Dataset();
        if (id) dataset.id = id;
        dataset.internalName = internalName;
        if (user) {
            dataset.createdBy = user;
            dataset.modifiedBy = user;
        }
        dataset.live = false;
        return dataset;
    }

    public addCode(code: string) {
        if (code.length > 12) {
            throw new Error('Code is to long');
        }
        this.code = code.toUpperCase();
    }

    public async addDatafile(file: Datafile) {
        file.dataset = this;
        await dbManager.getEntityManager().save(file);
    }

    public addLookuptables(lookupTable: LookupTable) {
        dbManager.getEntityManager().save(lookupTable);
    }

    public addTitleByString(title: string, lang: string) {
        dbManager.getEntityManager().save(DatasetTitle.datasetTitleFromString(this, title, lang));
    }

    public addTitle(title: DatasetTitle) {
        dbManager.getEntityManager().save(title);
    }

    public addDescriptionByString(description: string, lang: string) {
        dbManager.getEntityManager().save(DatasetDescription.datasetDescriptionFromString(this, description, lang));
    }

    public addDescription(description: DatasetDescription) {
        dbManager.getEntityManager().save(description);
    }
}
