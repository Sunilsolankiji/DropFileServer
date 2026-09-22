import express from 'express';
import { config } from '../config/index.js';
import { LOCAL_IP, getPublicBaseUrl } from '../utils/network.js';
import { peers, files, rooms, transfers } from '../infrastructure/state.js';

const router = express.Router();

router.get('/api/server-info', (req, res) => {
  res.json({
    ip: LOCAL_IP,
    port: config.PORT,
    publicBaseUrl: getPublicBaseUrl(),
    environment: config.NODE_ENV,
    limits: {
      maxFileSize: config.MAX_FILE_SIZE,
      defaultChunkSize: config.DEFAULT_CHUNK_SIZE,
      maxChunkSize: config.MAX_CHUNK_SIZE,
      maxInflightChunks: config.MAX_INFLIGHT_CHUNKS,
      maxTransferBufferBytes: config.MAX_TRANSFER_BUFFER_BYTES
    },
    stats: {
      totalPeers: peers.size,
      totalFiles: files.size,
      totalRooms: rooms.size,
      totalTransfers: transfers.size
    }
  });
});

export default router;