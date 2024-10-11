import fs from 'fs';

import { parse } from 'csv';
import { Seeder } from '@jorgebodega/typeorm-seeding';
import { DataSource } from 'typeorm';

import { Category } from '../entities/reference-data/category';
import { CategoryInfo } from '../entities/reference-data/category-info';
import { CategoryKey } from '../entities/reference-data/category-key';
import { CategoryKeyInfo } from '../entities/reference-data/category-key-info';
import { Hierarchy } from '../entities/reference-data/hierarchy';
import { ReferenceData } from '../entities/reference-data/reference-data';
import { ReferenceDataInfo } from '../entities/reference-data/reference-data-info';

export default class ReferenceDataSeeder extends Seeder {
    private async loadCategoriesIntoDatabase(dataSource: DataSource) {
        const categoriesCSV = `${__dirname}/../resources/reference-data/v1/categories.csv`;
        const categories: Category[] = [];
        const processCategoriesFile = async () => {
            const parser = fs
                .createReadStream(categoriesCSV)
                .pipe(parse({ delimiter: ',', bom: true, skip_empty_lines: true, columns: true }));
            for await (const row of parser) {
                const category = new Category();
                category.category = row.category;
                categories.push(category);
            }
        };
        await processCategoriesFile();
        await dataSource.createEntityManager().save<Category>(categories);
    }

    private async loadCategoryInfoIntoDatabase(dataSource: DataSource) {
        const categoryInfoCSV = `${__dirname}/../resources/reference-data/v1/category_info.csv`;
        const categoryInfos: CategoryInfo[] = [];
        const processCategoryInfoFile = async () => {
            const parser = fs
                .createReadStream(categoryInfoCSV)
                .pipe(parse({ delimiter: ',', bom: true, skip_empty_lines: true, columns: true }));
            for await (const row of parser) {
                const categoryInfo = new CategoryInfo();
                categoryInfo.category = row.category;
                categoryInfo.lang = row.lang;
                categoryInfo.description = row.description;
                categoryInfo.notes = row.notes;
                categoryInfos.push(categoryInfo);
            }
        };
        await processCategoryInfoFile();
        await dataSource.createEntityManager().save<CategoryInfo>(categoryInfos);
    }

    private async loadCategoryKeysIntoDatabase(dataSource: DataSource) {
        const categoryKeysCSV = `${__dirname}/../resources/reference-data/v1/category_key.csv`;
        const categoryKeys: CategoryKey[] = [];
        const processCategoryKeysFile = async () => {
            const parser = fs
                .createReadStream(categoryKeysCSV)
                .pipe(parse({ delimiter: ',', bom: true, skip_empty_lines: true, columns: true }));
            for await (const row of parser) {
                const categoryKey = new CategoryKey();
                categoryKey.category = row.category;
                categoryKey.categoryKey = row.category_key;
                categoryKeys.push(categoryKey);
            }
        };
        await processCategoryKeysFile();
        await dataSource.createEntityManager().save<CategoryKey>(categoryKeys);
    }

    private async loadCategoryKeyInfoIntoDatabase(dataSource: DataSource) {
        const categoryKeyInfoCSV = `${__dirname}/../resources/reference-data/v1/category_key_info.csv`;
        const categoryKeyInfos: CategoryKeyInfo[] = [];
        const processCategoryKeyInfosFile = async () => {
            const parser = fs
                .createReadStream(categoryKeyInfoCSV)
                .pipe(parse({ delimiter: ',', bom: true, skip_empty_lines: true, columns: true }));
            for await (const row of parser) {
                const categoryKeyInfo = new CategoryKeyInfo();
                categoryKeyInfo.lang = row.lang;
                categoryKeyInfo.description = row.description;
                categoryKeyInfo.notes = row.notes;
                categoryKeyInfo.categoryKey = row.category_key;
                categoryKeyInfos.push(categoryKeyInfo);
            }
        };
        await processCategoryKeyInfosFile();
        await dataSource.createEntityManager().save<CategoryKeyInfo>(categoryKeyInfos);
    }

    private async loadReferenceDataIntoDatabase(dataSource: DataSource) {
        const referenceDataCSV = `${__dirname}/../resources/reference-data/v1/reference_data.csv`;
        const processReferenceDataFile = async () => {
            const parser = fs
                .createReadStream(referenceDataCSV)
                .pipe(parse({ delimiter: ',', bom: true, skip_empty_lines: true, columns: true }));
            for await (const row of parser) {
                const referenceDataPoint = new ReferenceData();
                referenceDataPoint.itemId = row.item_id;
                referenceDataPoint.versionNumber = Number(row.version_no);
                referenceDataPoint.sortOrder = row.sort_order || null;
                referenceDataPoint.categoryKey = row.category_key;
                if (row.validity_start) {
                    const startYear = Number(row.validity_start.split('/')[2]);
                    const startMonth = Number(row.validity_start.split('/')[1]);
                    const startDay = Number(row.validity_start.split('/')[0]);
                    referenceDataPoint.validityStart = new Date(startYear, startMonth, startDay);
                }
                if (row.validity_end) {
                    const endYear = Number(row.validity_end.split('/')[2]);
                    const endMonth = Number(row.validity_end.split('/')[1]);
                    const endDay = Number(row.validity_end.split('/')[0]);
                    referenceDataPoint.validityEnd = new Date(endYear, endMonth, endDay);
                }
                await referenceDataPoint.save();
            }
        };
        await processReferenceDataFile();
    }

    private async loadReferenceDataInfoIntoDatabase(dataSource: DataSource) {
        const referenceDataInfoCSV = `${__dirname}/../resources/reference-data/v1/reference_data_info.csv`;
        const processReferenceDataInfosFile = async () => {
            const parser = fs
                .createReadStream(referenceDataInfoCSV)
                .pipe(parse({ delimiter: ',', bom: true, skip_empty_lines: true, columns: true }));
            for await (const row of parser) {
                const referenceDataInfo = new ReferenceDataInfo();
                referenceDataInfo.itemId = row.item_id;
                referenceDataInfo.versionNumber = row.version_no;
                referenceDataInfo.categoryKey = row.category_key;
                referenceDataInfo.lang = row.lang;
                referenceDataInfo.description = row.description;
                referenceDataInfo.notes = row.notes;
                await referenceDataInfo.save();
            }
        };
        await processReferenceDataInfosFile();
    }

    private async loadHierarchyIntoDatabase(dataSource: DataSource) {
        const hierarchyCSV = `${__dirname}/../resources/reference-data/v1/hierarchy.csv`;
        const hierarchies: Hierarchy[] = [];
        const processHierarchyFile = async () => {
            const parser = fs
                .createReadStream(hierarchyCSV)
                .pipe(parse({ delimiter: ',', bom: true, skip_empty_lines: true, columns: true }));
            for await (const row of parser) {
                const hierarchy = new Hierarchy();
                hierarchy.itemId = row.item_id;
                hierarchy.versionNumber = row.version_no;
                hierarchy.categoryKey = row.category_key;
                hierarchy.parentId = row.parent_id;
                hierarchy.parentVersion = row.parent_version;
                hierarchy.parentCategory = row.parent_category;
                hierarchies.push(hierarchy);
            }
        };
        await processHierarchyFile();
        await dataSource.createEntityManager().save<Hierarchy>(hierarchies);
    }

    async run(dataSource: DataSource) {
        await this.loadCategoriesIntoDatabase(dataSource);
        await this.loadCategoryInfoIntoDatabase(dataSource);
        await this.loadCategoryKeysIntoDatabase(dataSource);
        await this.loadCategoryKeyInfoIntoDatabase(dataSource);
        await this.loadReferenceDataIntoDatabase(dataSource);
        await this.loadReferenceDataInfoIntoDatabase(dataSource);
        await this.loadHierarchyIntoDatabase(dataSource);
    }
}
