// DO NOT REMOVE!
// DuckDB handles numbers as bigints.  BigInts don't serialise
// toJSON easily.  This monkypatches BigInt so that if the number
// is less than the max safe interger we return a number otherwise
// we return a string

// eslint-disable-next-line @typescript-eslint/no-explicit-any
(BigInt.prototype as any).toJSON = function (): number | string {
  if (this < Number.MAX_SAFE_INTEGER) return Number(this);
  return this.toString();
};
