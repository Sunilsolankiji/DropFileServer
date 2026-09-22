import healthRouter from './health.js';
import serverInfoRouter from './serverInfo.js';
import roomsRouter from './rooms.js';
import { createTransfersRouter } from './transfers.js';
import { createTransferChunksRouter } from './transferChunks.js';

export function registerRoutes(app, io, transferLifecycle) {
  app.use(healthRouter);
  app.use(serverInfoRouter);
  app.use(roomsRouter);
  app.use(createTransfersRouter(transferLifecycle));
  app.use(createTransferChunksRouter(io, transferLifecycle));
}