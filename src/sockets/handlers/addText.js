import { v4 as uuidv4 } from 'uuid';
import { log } from '../../utils/logger.js';
import { safeCallback } from '../../utils/safeCallback.js';
import { isValidRoomCode, isValidTextMessage } from '../../utils/validation.js';
import { checkSocketRateLimit } from '../../services/peerService.js';
import { getRoomTexts, mapTextToResponse } from '../../services/textService.js';
import { peers, rooms, roomTexts } from '../../infrastructure/state.js';

export function handleAddText(socket) {
  socket.on('add-text', ({ roomCode, text }, callback) => {
    try {
      if (!checkSocketRateLimit(socket.id)) {
        return safeCallback(callback, { success: false, error: 'Rate limit exceeded' });
      }

      if (!isValidRoomCode(roomCode)) {
        return safeCallback(callback, { success: false, error: 'Invalid room code' });
      }
      if (!isValidTextMessage(text)) {
        return safeCallback(callback, { success: false, error: 'Invalid text message' });
      }

      const roomPeers = rooms.get(roomCode);
      if (!roomPeers) {
        return safeCallback(callback, { success: false, error: 'Room not found' });
      }

      const currentPeer = peers.get(socket.id);
      if (!currentPeer || currentPeer.roomCode !== roomCode || currentPeer.id !== text.peerId) {
        return safeCallback(callback, { success: false, error: 'Peer not registered in this room' });
      }

      const messageContent = typeof text.text === 'string' ? text.text : text.message;
      const message = {
        id: typeof text.id === 'string' && text.id ? text.id : uuidv4(),
        roomCode,
        text: messageContent.trim(),
        message: messageContent.trim(),
        peerId: currentPeer.id,
        peerName: currentPeer.name,
        createdAt: text.createdAt && !Number.isNaN(Date.parse(text.createdAt))
          ? text.createdAt
          : new Date().toISOString(),
        ownerSocketId: socket.id
      };

      const messages = getRoomTexts(roomCode);
      messages.push(message);
      roomTexts.set(roomCode, messages);

      socket.to(roomCode).emit('text-added', mapTextToResponse(message));

      safeCallback(callback, {
        success: true,
        message: mapTextToResponse(message)
      });

      log('info', 'Text added', { messageId: message.id, peerName: message.peerName, roomCode });
    } catch (error) {
      log('error', 'Error in add-text', { error: error.message });
      safeCallback(callback, { success: false, error: error.message });
    }
  });
}