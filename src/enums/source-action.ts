/* eslint-disable no-shadow */
/* eslint-disable no-unused-vars */
export enum SourceAction {
    CREATE = 'CREATE',
    APPEND = 'APPEND',
    TRUNCATE = 'TRUNCATE-THEN-LOAD',
    IGNORE = 'IGNORE',
    UNKNOWN = 'UNKNOWN'
}
