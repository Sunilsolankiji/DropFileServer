import { log } from '../../utils/logger.js';
import { safeCallback } from '../../utils/safeCallback.js';
import { isValidRoomCode } from '../../utils/validation.js';
import { getTransferChunkUrlTemplate } from '../../utils/network.js';
import { checkSocketRateLimit, getPeerInRoom } from '../../services/peerService.js';
import { getFileOrThrow } from '../../services/fileService.js';
import { mapTransferStatus, updateTransferTimestamp } from '../../services/transferService.js';
import { files } from '../../infrastructure/state.js';

export function handleStartTransfer(io, socket, transferLifecycle) {
  socket.on('start-transfer', ({ roomCode, fileId, peerId }, callback) => {
    try {
      if (!checkSocketRateLimit(socket.id)) {
        return safeCallback(callback, { success: false, error: 'Rate limit exceeded' });
      }

      if (!isValidRoomCode(roomCode)) {
        return safeCallback(callback, { success: false, error: 'Invalid room code' });
      }

      const peer = getPeerInRoom(socket, roomCode, peerId);
      const file = getFileOrThrow(fileId);

      if (file.roomCode !== roomCode) {
        return safeCallback(callback, { success: false, error: 'File not in this room' });
      }

      const transfer = transferLifecycle.getTransferOrThrow(file.transferId);
      if (transfer.senderPeerId === peer.id) {
        return safeCallback(callback, { success: false, error: 'Sender cannot start download for own file' });
      }
      transfer.receivers.set(peer.id, {
        peerId: peer.id,
        socketId: socket.id,
        joinedAt: Date.now(),
        lastSeen: Date.now()
      });
      transfer.state = 'transferring';
      updateTransferTimestamp(transfer);

      const fileRecord = files.get(fileId);
      if (fileRecord) {
        fileRecord.status = transfer.state;
      }

      io.to(roomCode).emit('transfer-updated', {
        transferId: transfer.id,
        fileId,
        status: mapTransferStatus(transfer)
      });

      safeCallback(callback, {
        success: true,
        transferId: transfer.id,
        fileId,
        chunkSize: transfer.chunkSize,
        totalChunks: transfer.totalChunks,
        state: transfer.state,
        downloadUrlTemplate: getTransferChunkUrlTemplate(transfer.id),
        uploadedChunkIndexes: Array.from(transfer.chunkStates.uploaded).sort((a, b) => a - b),
        acknowledgedChunkIndexes: Array.from(transfer.chunkStates.acknowledged).sort((a, b) => a - b),
        activeReceivers: transfer.receivers.size
      });
    } catch (error) {
      log('error', 'Error in start-transfer', { error: error.message });
      safeCallback(callback, { success: false, error: error.message });
    }
  });
}