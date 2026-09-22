import { peers, rooms } from '../../infrastructure/state.js';

export function handleHeartbeat(socket) {
  socket.on('heartbeat', ({ roomCode }) => {
    const peerInfo = peers.get(socket.id);
    if (peerInfo) {
      peerInfo.lastSeen = Date.now();
      peerInfo.isActive = true;

      const roomPeers = rooms.get(roomCode);
      if (roomPeers) {
        const index = roomPeers.findIndex(peer => peer.socketId === socket.id);
        if (index > -1) {
          roomPeers[index].lastSeen = Date.now();
        }
      }
    }
  });
}