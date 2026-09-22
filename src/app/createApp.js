import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import rateLimit from 'express-rate-limit';
import { config } from '../config/index.js';

export function createApp() {
  const app = express();

  app.use(helmet({
    crossOriginResourcePolicy: { policy: 'cross-origin' }
  }));

  const apiLimiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 300,
    skip: req => req.method === 'OPTIONS' || req.path.startsWith('/api/transfers/') && /\/chunks\/\d+$/.test(req.path),
    message: { error: 'Too many requests, please try again later.' }
  });

  app.use(cors(config.corsOptions));
  app.options('*', cors(config.corsOptions));
  app.use('/api/', apiLimiter);
  app.use(express.json({ limit: '2mb' }));
  app.use(express.urlencoded({ limit: '2mb', extended: true }));
  app.use('/api/transfers/:transferId/chunks/:chunkIndex', express.raw({
    type: '*/*',
    limit: `${config.MAX_CHUNK_SIZE}b`
  }));

  return app;
}