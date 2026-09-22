import { log } from '../../utils/logger.js';
import { safeCallback } from '../../utils/safeCallback.js';
import { isValidRoomCode, isValidPeerName } from '../../utils/validation.js';
import { checkSocketRateLimit } from '../../services/peerService.js';
import { peers, rooms } from '../../infrastructure/state.js';

export function handleUpdatePeerName(io, socket) {
  socket.on('update-peer-name', ({ roomCode, peerId, peerName }, callback) => {
    try {
      if (!checkSocketRateLimit(socket.id)) {
        return safeCallback(callback, { success: false, error: 'Rate limit exceeded' });
      }
      if (!isValidRoomCode(roomCode)) {
        return safeCallback(callback, { success: false, error: 'Invalid room code' });
      }
      if (!peerId || typeof peerId !== 'string') {
        return safeCallback(callback, { success: false, error: 'Invalid peer ID' });
      }
      if (!isValidPeerName(peerName)) {
        return safeCallback(callback, { success: false, error: 'Invalid peer name' });
      }

      const currentPeer = peers.get(socket.id);
      if (!currentPeer || currentPeer.roomCode !== roomCode || currentPeer.id !== peerId) {
        return safeCallback(callback, { success: false, error: 'Peer not registered in this room' });
      }

      const roomPeers = rooms.get(roomCode);
      if (!roomPeers) {
        return safeCallback(callback, { success: false, error: 'Room not found' });
      }

      const updatedName = peerName.trim().substring(0, 50);
      currentPeer.name = updatedName;
      currentPeer.lastSeen = Date.now();

      const peerIndex = roomPeers.findIndex(peer => peer.id === peerId);
      if (peerIndex === -1) {
        return safeCallback(callback, { success: false, error: 'Peer not found in room' });
      }

      roomPeers[peerIndex] = {
        ...roomPeers[peerIndex],
        name: updatedName,
        lastSeen: currentPeer.lastSeen
      };

      const updatedPeer = roomPeers[peerIndex];
      io.to(roomCode).emit('peer-updated', updatedPeer);

      safeCallback(callback, { success: true, peer: updatedPeer });
      log('info', 'Peer name updated', { peerId, peerName: updatedName, roomCode });
    } catch (error) {
      log('error', 'Error in update-peer-name', { error: error.message });
      safeCallback(callback, { success: false, error: error.message });
    }
  });
}