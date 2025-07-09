import helmet from 'helmet';

export const strictTransport = helmet({
  hsts: {
    maxAge: 63072000, // 2 years in seconds
    includeSubDomains: true,
    preload: true
  }
});
