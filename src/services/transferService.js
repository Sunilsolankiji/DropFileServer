import { v4 as uuidv4 } from 'uuid';
import { config } from '../config/index.js';
import { peers, files, transfers } from '../infrastructure/state.js';
import { normalizeChunkSize } from '../utils/validation.js';

export function mapTransferStatus(transfer) {
  const completedReceiverPeerIds = [];
  for (const [peerId, acknowledgedChunks] of transfer.chunkStates.acknowledgedByPeer) {
    if (acknowledgedChunks.size === transfer.totalChunks) {
      completedReceiverPeerIds.push(peerId);
    }
  }

  return {
    state: transfer.state,
    uploadedChunks: transfer.chunkStates.uploaded.size,
    acknowledgedChunks: transfer.chunkStates.acknowledged.size,
    activeReceivers: transfer.receivers.size,
    receiverPeerIds: Array.from(transfer.receivers.keys()),
    completedReceiverPeerIds,
    senderConnected: Boolean(transfer.senderSocketId && peers.has(transfer.senderSocketId))
  };
}

export function getBufferedBytes(transfer) {
  let total = 0;
  for (const chunkInfo of transfer.chunkBuffer.values()) {
    total += chunkInfo.data.length;
  }
  return total;
}

export function isTransferExpired(transfer) {
  return transfer.expiresAt < Date.now();
}

export function canAcceptChunk(transfer, chunkIndex) {
  if (transfer.chunkBuffer.has(chunkIndex)) {
    return false;
  }

  if (transfer.chunkBuffer.size >= config.MAX_INFLIGHT_CHUNKS) {
    return false;
  }

  return getBufferedBytes(transfer) < config.MAX_TRANSFER_BUFFER_BYTES;
}

export function updateTransferTimestamp(transfer) {
  transfer.updatedAt = Date.now();
}

export function getTransferChunksSummary(transfer) {
  return {
    totalChunks: transfer.totalChunks,
    uploadedChunks: Array.from(transfer.chunkStates.uploaded).sort((a, b) => a - b),
    acknowledgedChunks: Array.from(transfer.chunkStates.acknowledged).sort((a, b) => a - b)
  };
}

export function createTransferRecord({ roomCode, file, peer }) {
  const fileId = file.id || uuidv4();
  const transferId = uuidv4();
  const chunkSize = normalizeChunkSize(file.chunkSize);
  const totalChunks = file.totalChunks && Number.isInteger(file.totalChunks)
    ? file.totalChunks
    : Math.ceil(file.size / chunkSize);
  const now = Date.now();
  const expiresAt = now + config.FILE_TTL_MS;

  if (!Number.isInteger(totalChunks) || totalChunks <= 0) {
    throw new Error('Invalid total chunks');
  }

  const fileData = {
    id: fileId,
    name: file.name.substring(0, 255),
    size: file.size,
    type: file.type || 'application/octet-stream',
    peerId: peer.id,
    peerName: peer.name,
    roomCode,
    expiresAt,
    uploadedAt: now,
    ownerSocketId: peer.socketId,
    transferId,
    totalChunks,
    chunkSize,
    hash: typeof file.hash === 'string' ? file.hash : null,
    status: 'pending'
  };

  const transfer = {
    id: transferId,
    fileId,
    roomCode,
    senderPeerId: peer.id,
    senderSocketId: peer.socketId,
    senderName: peer.name,
    fileName: fileData.name,
    fileSize: fileData.size,
    mimeType: fileData.type,
    hash: fileData.hash,
    chunkSize,
    totalChunks,
    state: 'pending',
    createdAt: now,
    updatedAt: now,
    expiresAt,
    chunkStates: {
      uploaded: new Set(),
      acknowledged: new Set(),
      acknowledgedByPeer: new Map(),
      chunkAcknowledgements: new Map()
    },
    chunkBuffer: new Map(),
    chunkUploaders: new Set(),
    receivers: new Map()
  };

  files.set(fileId, fileData);
  transfers.set(transferId, transfer);
  return { fileData, transfer };
}

export function createTransferLifecycle(io) {
  function cancelTransfer(transfer, reason, notifyRoom = true) {
    if (!transfer || transfer.state === 'completed' || transfer.state === 'cancelled' || transfer.state === 'expired') {
      return;
    }

    transfer.state = reason === 'expired' ? 'expired' : 'cancelled';
    transfer.cancelReason = reason;
    transfer.chunkBuffer.clear();

    const file = files.get(transfer.fileId);
    if (file) {
      file.status = transfer.state;
    }

    if (notifyRoom) {
      io.to(transfer.roomCode).emit('transfer-updated', {
        transferId: transfer.id,
        fileId: transfer.fileId,
        status: mapTransferStatus(transfer),
        reason
      });
    }
  }

  function completeTransfer(transfer) {
    transfer.state = 'completed';

    const file = files.get(transfer.fileId);
    if (file) {
      file.status = 'completed';
    }

    io.to(transfer.roomCode).emit('transfer-completed', {
      transferId: transfer.id,
      fileId: transfer.fileId,
      status: mapTransferStatus(transfer)
    });
  }

  function removeReceiverFromTransfer(transfer, receiverPeerId, reason, notifyRoom = true) {
    if (!transfer.receivers.has(receiverPeerId)) {
      return false;
    }

    transfer.receivers.delete(receiverPeerId);
    transfer.chunkStates.acknowledgedByPeer.delete(receiverPeerId);

    for (const [chunkIndex, acknowledgedPeers] of transfer.chunkStates.chunkAcknowledgements) {
      acknowledgedPeers.delete(receiverPeerId);
      if (acknowledgedPeers.size === 0) {
        transfer.chunkStates.chunkAcknowledgements.delete(chunkIndex);
      }
      if (acknowledgedPeers.size < transfer.receivers.size) {
        transfer.chunkStates.acknowledged.delete(chunkIndex);
      }
      if (acknowledgedPeers.size >= transfer.receivers.size && transfer.chunkStates.uploaded.has(chunkIndex)) {
        transfer.chunkBuffer.delete(chunkIndex);
      }
    }

    transfer.state = transfer.receivers.size > 0 ? 'transferring' : transfer.chunkStates.uploaded.size > 0 ? 'ready' : 'pending';
    if (transfer.receivers.size === 0 && transfer.chunkStates.acknowledged.size === transfer.totalChunks) {
      completeTransfer(transfer);
    }
    updateTransferTimestamp(transfer);

    const file = files.get(transfer.fileId);
    if (file) {
      file.status = transfer.state;
    }

    if (notifyRoom) {
      io.to(transfer.roomCode).emit('transfer-updated', {
        transferId: transfer.id,
        fileId: transfer.fileId,
        status: mapTransferStatus(transfer),
        reason,
        peerId: receiverPeerId
      });
    }

    return true;
  }

  function getTransferOrThrow(transferId) {
    const transfer = transfers.get(transferId);
    if (!transfer) {
      throw new Error('Transfer not found');
    }
    if (isTransferExpired(transfer)) {
      cancelTransfer(transfer, 'expired');
      throw new Error('Transfer has expired');
    }
    return transfer;
  }

  return { cancelTransfer, completeTransfer, removeReceiverFromTransfer, getTransferOrThrow };
}