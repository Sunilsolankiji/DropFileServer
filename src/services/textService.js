import { roomTexts } from '../infrastructure/state.js';

export function getRoomTexts(roomCode) {
  return roomTexts.get(roomCode) || [];
}

export function mapTextToResponse(message) {
  return {
    id: message.id,
    text: message.text,
    message: message.message,
    peerId: message.peerId,
    peerName: message.peerName,
    createdAt: message.createdAt
  };
}