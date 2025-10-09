import { CubeBuildStatus } from '../enums/cube-build-status';
import { Locale } from '../enums/locale';
import { TransactionBlock } from './transaction-block';

export interface CubeBuilder {
  buildStatus: CubeBuildStatus;
  transactionBlocks: TransactionBlock[];
  coreViewSQL: Map<Locale, string>;
}
