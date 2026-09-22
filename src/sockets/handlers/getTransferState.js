import { log } from '../../utils/logger.js';
import { safeCallback } from '../../utils/safeCallback.js';
import { checkSocketRateLimit } from '../../services/peerService.js';
import { getTransferChunksSummary } from '../../services/transferService.js';

export function handleGetTransferState(socket, transferLifecycle) {
  socket.on('get-transfer-state', ({ transferId }, callback) => {
    try {
      if (!checkSocketRateLimit(socket.id)) {
        return safeCallback(callback, { success: false, error: 'Rate limit exceeded' });
      }

      const transfer = transferLifecycle.getTransferOrThrow(transferId);
      safeCallback(callback, {
        success: true,
        transferId,
        fileId: transfer.fileId,
        roomCode: transfer.roomCode,
        state: transfer.state,
        chunkSize: transfer.chunkSize,
        summary: getTransferChunksSummary(transfer),
        uploadedChunkIndexes: Array.from(transfer.chunkStates.uploaded).sort((a, b) => a - b),
        acknowledgedChunkIndexes: Array.from(transfer.chunkStates.acknowledged).sort((a, b) => a - b)
      });
    } catch (error) {
      log('error', 'Error in get-transfer-state', { error: error.message });
      safeCallback(callback, { success: false, error: error.message });
    }
  });
}