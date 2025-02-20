import { User } from '../../entities/user/user';
import { Locale } from '../../enums/locale';
import { DatasetService } from '../../services/dataset';

declare module 'express-serve-static-core' {
    interface Request {
        user?: User;
        datasetService: DatasetService;
    }
}
