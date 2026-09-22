import { log } from '../../utils/logger.js';
import { safeCallback } from '../../utils/safeCallback.js';
import { isValidRoomCode, isValidPeerName } from '../../utils/validation.js';
import { getServerInfo } from '../../utils/network.js';
import { checkSocketRateLimit } from '../../services/peerService.js';
import { getRoomTexts, mapTextToResponse } from '../../services/textService.js';
import { getRoomFiles } from '../../services/fileService.js';
import { peers, rooms, roomTexts } from '../../infrastructure/state.js';

export function handleJoinRoom(socket) {
  socket.on('join-room', ({ roomCode, peerName, peerId }, callback) => {
    try {
      if (!checkSocketRateLimit(socket.id)) {
        return safeCallback(callback, { success: false, error: 'Rate limit exceeded' });
      }

      if (!isValidRoomCode(roomCode)) {
        return safeCallback(callback, { success: false, error: 'Invalid room code format' });
      }
      if (peerName && !isValidPeerName(peerName)) {
        return safeCallback(callback, { success: false, error: 'Invalid peer name' });
      }
      if (!peerId || typeof peerId !== 'string') {
        return safeCallback(callback, { success: false, error: 'Invalid peer ID' });
      }

      if (peers.has(socket.id)) {
        const existingPeer = peers.get(socket.id);
        log('warn', 'Duplicate join-room attempt', { socketId: socket.id, existingName: existingPeer.name });

        const roomPeers = rooms.get(roomCode) || [];
        const otherPeers = roomPeers.filter(peer => peer.socketId !== socket.id);
        const texts = getRoomTexts(roomCode)
          .slice()
          .sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt))
          .map(mapTextToResponse);

        return safeCallback(callback, {
          success: true,
          peers: otherPeers,
          files: getRoomFiles(roomCode),
          texts,
          serverInfo: getServerInfo()
        });
      }

      socket.join(roomCode);

      if (!rooms.has(roomCode)) {
        rooms.set(roomCode, []);
      }
      if (!roomTexts.has(roomCode)) {
        roomTexts.set(roomCode, []);
      }

      const sanitizedName = peerName ? peerName.trim().substring(0, 50) : `Device ${peerId.slice(0, 6)}`;
      const peerInfo = {
        id: peerId,
        socketId: socket.id,
        name: sanitizedName,
        joinedAt: Date.now(),
        lastSeen: Date.now(),
        ip: socket.handshake.address,
        isActive: true
      };

      const roomPeers = rooms.get(roomCode);
      const existingPeerIndex = roomPeers.findIndex(peer => peer.id === peerId);
      const isNewPeer = existingPeerIndex === -1;

      if (existingPeerIndex > -1) {
        const oldSocketId = roomPeers[existingPeerIndex].socketId;
        if (oldSocketId !== socket.id) {
          peers.delete(oldSocketId);
        }
        roomPeers[existingPeerIndex] = peerInfo;
      } else {
        roomPeers.push(peerInfo);
      }

      peers.set(socket.id, { ...peerInfo, roomCode });

      if (isNewPeer) {
        socket.to(roomCode).emit('peer-joined', peerInfo);
      }

      const otherPeers = roomPeers.filter(peer => peer.socketId !== socket.id);
      const texts = getRoomTexts(roomCode)
        .slice()
        .sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt))
        .map(mapTextToResponse);

      safeCallback(callback, {
        success: true,
        peers: otherPeers,
        files: getRoomFiles(roomCode),
        texts,
        serverInfo: getServerInfo()
      });

      log('info', 'Peer joined room', {
        name: peerInfo.name,
        roomCode,
        isNew: isNewPeer,
        roomPeers: roomPeers.length
      });
    } catch (error) {
      log('error', 'Error in join-room', { error: error.message });
      safeCallback(callback, { success: false, error: error.message });
    }
  });
}