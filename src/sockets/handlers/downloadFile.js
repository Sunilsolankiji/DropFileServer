import { log } from '../../utils/logger.js';
import { safeCallback } from '../../utils/safeCallback.js';
import { getTransferChunkUrlTemplate } from '../../utils/network.js';
import { getFileOrThrow, mapFileToResponse } from '../../services/fileService.js';

export function handleDownloadFile(socket, transferLifecycle) {
  socket.on('download-file', ({ fileId, roomCode, peerId }, callback) => {
    try {
      const file = getFileOrThrow(fileId);
      const transfer = transferLifecycle.getTransferOrThrow(file.transferId);
      if (!peerId || typeof peerId !== 'string') {
        return safeCallback(callback, { success: false, error: 'Invalid peer ID' });
      }
      return safeCallback(callback, {
        success: true,
        file: mapFileToResponse(file),
        transfer: {
          transferId: transfer.id,
          chunkSize: transfer.chunkSize,
          totalChunks: transfer.totalChunks,
          state: transfer.state,
          roomCode: file.roomCode,
          startRequired: !transfer.receivers.has(peerId),
          downloadUrlTemplate: getTransferChunkUrlTemplate(transfer.id),
          uploadedChunkIndexes: Array.from(transfer.chunkStates.uploaded).sort((a, b) => a - b),
          acknowledgedChunkIndexes: Array.from(transfer.chunkStates.acknowledged).sort((a, b) => a - b)
        }
      });
    } catch (error) {
      log('error', 'Error in download-file', { error: error.message });
      safeCallback(callback, { success: false, error: error.message });
    }
  });
}