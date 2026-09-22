import { peers, socketRateLimits } from '../infrastructure/state.js';
import { config } from '../config/index.js';

export function getPeerBySocket(socketId) {
  return peers.get(socketId) || null;
}

export function getPeerInRoom(socket, roomCode, peerId) {
  const currentPeer = getPeerBySocket(socket.id);
  if (!currentPeer) {
    throw new Error('Peer not registered');
  }
  if (currentPeer.roomCode !== roomCode) {
    throw new Error('Peer not registered in this room');
  }
  if (peerId && currentPeer.id !== peerId) {
    throw new Error('Peer ID does not match current connection');
  }
  return currentPeer;
}

export function checkSocketRateLimit(socketId) {
  const now = Date.now();
  let rateInfo = socketRateLimits.get(socketId);

  if (!rateInfo || now > rateInfo.resetTime) {
    rateInfo = { count: 0, resetTime: now + config.SOCKET_RATE_WINDOW };
    socketRateLimits.set(socketId, rateInfo);
  }

  rateInfo.count++;
  return rateInfo.count <= config.SOCKET_RATE_LIMIT;
}