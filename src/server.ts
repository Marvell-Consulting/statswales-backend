import dotenv from 'dotenv';
import 'reflect-metadata';

import app, { databaseManager } from './app';
import { datasourceOptions } from './data-source';

dotenv.config();

const PORT = process.env.BACKEND_PORT || 3000;

databaseManager(datasourceOptions);

app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});
