import { files, transfers } from '../infrastructure/state.js';
import { mapTransferStatus } from './transferService.js';

export function mapFileToResponse(file) {
  return {
    id: file.id,
    name: file.name,
    size: file.size,
    type: file.type,
    peerId: file.peerId,
    peerName: file.peerName,
    roomCode: file.roomCode,
    expiresAt: file.expiresAt,
    uploadedAt: file.uploadedAt,
    transferId: file.transferId,
    totalChunks: file.totalChunks,
    chunkSize: file.chunkSize,
    status: file.status,
    hash: file.hash,
    transfer: file.transferId ? mapTransferStatus(transfers.get(file.transferId) || {
      state: 'expired',
      chunkStates: {
        uploaded: new Set(),
        acknowledged: new Set(),
        acknowledgedByPeer: new Map(),
        chunkAcknowledgements: new Map()
      },
      receivers: new Map(),
      senderSocketId: null
    }) : undefined
  };
}

export function getRoomFiles(roomCode) {
  return Array.from(files.values())
    .filter(file => file.roomCode === roomCode)
    .map(mapFileToResponse);
}

export function getFileOrThrow(fileId) {
  const file = files.get(fileId);
  if (!file) {
    throw new Error('File not found or expired');
  }
  if (file.expiresAt < Date.now()) {
    files.delete(fileId);
    if (file.transferId) {
      transfers.delete(file.transferId);
    }
    throw new Error('File has expired');
  }
  return file;
}