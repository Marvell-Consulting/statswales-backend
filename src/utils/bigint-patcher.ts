// DO NOT REMOVE!
// DuckDB handles numbers as bigints.  BigInts don't serialise
// toJSON easily.  This monkypatches BigInt so that if the number
// is less than the max safe interger we return a number otherwise
// we return a string
(BigInt.prototype as any).toJSON = function () {
  if (this < Number.MAX_SAFE_INTEGER) return Number(this);
  return this.toString();
};
