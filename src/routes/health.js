import express from 'express';
import { config } from '../config/index.js';
import { LOCAL_IP, getPublicBaseUrl } from '../utils/network.js';
import { peers, files, rooms, transfers } from '../infrastructure/state.js';

const router = express.Router();

router.get('/health', (req, res) => {
  const memoryUsage = process.memoryUsage();
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    environment: config.NODE_ENV,
    ip: LOCAL_IP,
    port: config.PORT,
    publicBaseUrl: getPublicBaseUrl(),
    uptime: Math.floor(process.uptime()),
    memory: {
      heapUsed: `${Math.round(memoryUsage.heapUsed / 1024 / 1024)}MB`,
      heapTotal: `${Math.round(memoryUsage.heapTotal / 1024 / 1024)}MB`,
      rss: `${Math.round(memoryUsage.rss / 1024 / 1024)}MB`
    },
    stats: {
      peers: peers.size,
      files: files.size,
      rooms: rooms.size,
      transfers: transfers.size
    }
  });
});

export default router;