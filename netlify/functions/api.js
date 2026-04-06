import serverless from 'serverless-http';

import app from '../../backend/app.js';

export const handler = serverless(app);
