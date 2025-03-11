export enum NumberType {
  Integer = 'integer',
  Decimal = 'decimal'
}

export interface NumberExtractor {
  type: NumberType;
  decimalPlaces: number;
}
