import http from 'http';
import { Server } from 'socket.io';
import { config } from '../config/index.js';
import { log } from '../utils/logger.js';
import { LOCAL_IP } from '../utils/network.js';
import { createApp } from './createApp.js';
import { notFoundHandler, errorHandler } from './errorHandlers.js';
import { registerRoutes } from '../routes/index.js';
import { registerSocketHandlers } from '../sockets/index.js';
import { startCleanupJobs } from './cleanup.js';
import { createTransferLifecycle } from '../services/transferService.js';

export function bootstrap() {
  const app = createApp();
  const server = http.createServer(app);

  const io = new Server(server, {
    cors: config.corsOptions,
    maxHttpBufferSize: config.MAX_CHUNK_SIZE + 64 * 1024
  });

  const transferLifecycle = createTransferLifecycle(io);

  registerSocketHandlers(io, transferLifecycle);
  registerRoutes(app, io, transferLifecycle);

  app.use(notFoundHandler);
  app.use(errorHandler);

  startCleanupJobs(io, transferLifecycle);

  server.listen(config.PORT, '0.0.0.0', () => {
    console.log(`
+===========================================================+
|                  DropFile Server                          |
+===========================================================+
| Status:   Running                                         |
| IP:       ${LOCAL_IP.padEnd(45)} |
| Port:     ${config.PORT.toString().padEnd(45)} |
| Env:      ${config.NODE_ENV.padEnd(45)} |
| URL:      http://${LOCAL_IP}:${config.PORT}
+===========================================================+
| Health:   http://${LOCAL_IP}:${config.PORT}/health
| Info:     http://${LOCAL_IP}:${config.PORT}/api/server-info
+===========================================================+
`);
  });

  function gracefulShutdown(signal) {
    log('info', `Shutting down server (${signal})...`);
    io.close();
    server.close(() => {
      log('info', 'Server stopped gracefully');
      process.exit(0);
    });

    setTimeout(() => {
      log('warn', 'Forcing shutdown after timeout');
      process.exit(1);
    }, 10000);
  }

  process.on('SIGINT', () => gracefulShutdown('SIGINT'));
  process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

  return { app, server, io };
}