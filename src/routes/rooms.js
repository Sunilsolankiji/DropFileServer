import express from 'express';
import { isValidRoomCode } from '../utils/validation.js';
import { rooms } from '../infrastructure/state.js';
import { getRoomTexts, mapTextToResponse } from '../services/textService.js';
import { getRoomFiles } from '../services/fileService.js';

const router = express.Router();

router.get('/api/rooms/:roomCode', (req, res) => {
  const { roomCode } = req.params;

  if (!isValidRoomCode(roomCode)) {
    return res.status(400).json({ error: 'Invalid room code format' });
  }

  const roomPeers = rooms.get(roomCode);
  if (!roomPeers) {
    return res.status(404).json({ error: 'Room not found' });
  }

  const texts = getRoomTexts(roomCode)
    .slice()
    .sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt))
    .map(mapTextToResponse);

  return res.json({
    roomCode,
    peers: roomPeers,
    files: getRoomFiles(roomCode),
    texts,
    timestamp: new Date().toISOString()
  });
});

export default router;