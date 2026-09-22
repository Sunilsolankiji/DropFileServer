import { log } from '../../utils/logger.js';
import { safeCallback } from '../../utils/safeCallback.js';
import { checkSocketRateLimit } from '../../services/peerService.js';
import { getFileOrThrow } from '../../services/fileService.js';
import { peers, files, transfers } from '../../infrastructure/state.js';

export function handleRemoveFile(io, socket, transferLifecycle) {
  socket.on('remove-file', ({ fileId, roomCode }, callback) => {
    try {
      if (!checkSocketRateLimit(socket.id)) {
        return safeCallback(callback, { success: false, error: 'Rate limit exceeded' });
      }

      const file = getFileOrThrow(fileId);
      if (file.roomCode !== roomCode) {
        return safeCallback(callback, { success: false, error: 'File not in this room' });
      }

      const peer = peers.get(socket.id);
      if (file.peerId !== peer?.id && file.ownerSocketId !== socket.id) {
        return safeCallback(callback, { success: false, error: 'Unauthorized: Only file owner can remove' });
      }

      if (file.transferId && transfers.has(file.transferId)) {
        transferLifecycle.cancelTransfer(transfers.get(file.transferId), 'removed');
        transfers.delete(file.transferId);
      }

      files.delete(fileId);
      io.to(roomCode).emit('file-removed', { fileId });

      safeCallback(callback, { success: true });
      log('info', 'File removed', { fileId, roomCode });
    } catch (error) {
      log('error', 'Error in remove-file', { error: error.message });
      safeCallback(callback, { success: false, error: error.message });
    }
  });
}