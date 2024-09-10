import 'dotenv/config';
import 'reflect-metadata';

import app, { initDb } from './app';

const PORT = process.env.BACKEND_PORT || 3000;

initDb();

app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});
