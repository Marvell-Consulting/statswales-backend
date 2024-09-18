export enum SourceAction {
    CREATE = 'CREATE',
    APPEND = 'APPEND',
    TRUNCATE = 'TRUNCATE-THEN-LOAD',
    IGNORE = 'IGNORE',
    UNKNOWN = 'UNKNOWN'
}
