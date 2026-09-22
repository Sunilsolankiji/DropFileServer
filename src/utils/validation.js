import { config } from '../config/index.js';

export function isValidRoomCode(roomCode) {
  if (!roomCode || typeof roomCode !== 'string') return false;
  return /^[a-zA-Z0-9_-]{3,50}$/.test(roomCode);
}

export function isValidPeerName(name) {
  if (!name || typeof name !== 'string') return false;
  return name.length >= 1 && name.length <= 50 && !/<[^>]*>/.test(name);
}

export function isValidFileMetadata(file) {
  if (!file || typeof file !== 'object') return false;
  if (!file.name || typeof file.name !== 'string' || file.name.length > 255) return false;
  if (typeof file.size !== 'number' || file.size <= 0 || file.size > config.MAX_FILE_SIZE) return false;
  if (file.type && typeof file.type !== 'string') return false;
  return true;
}

export function isValidTextMessage(message) {
  if (!message || typeof message !== 'object') return false;
  if (message.peerName && !isValidPeerName(message.peerName)) return false;

  const messageText = typeof message.text === 'string'
    ? message.text
    : typeof message.message === 'string'
      ? message.message
      : null;

  if (messageText === null) return false;

  const trimmedText = messageText.trim();
  return trimmedText.length >= 1 && trimmedText.length <= 2000;
}

export function parsePositiveInteger(value) {
  const parsed = Number.parseInt(value, 10);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : null;
}

export function normalizeChunkSize(chunkSize) {
  const requested = parsePositiveInteger(chunkSize);
  if (!requested || requested <= 0) {
    return config.DEFAULT_CHUNK_SIZE;
  }
  return Math.min(requested, config.MAX_CHUNK_SIZE);
}

export function isValidChunkIndex(index, totalChunks) {
  return Number.isInteger(index) && index >= 0 && index < totalChunks;
}