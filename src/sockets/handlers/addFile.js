import { log } from '../../utils/logger.js';
import { safeCallback } from '../../utils/safeCallback.js';
import { isValidRoomCode, isValidFileMetadata } from '../../utils/validation.js';
import { getTransferChunkUrlTemplate } from '../../utils/network.js';
import { checkSocketRateLimit, getPeerInRoom } from '../../services/peerService.js';
import { createTransferRecord, mapTransferStatus } from '../../services/transferService.js';
import { mapFileToResponse } from '../../services/fileService.js';
import { files } from '../../infrastructure/state.js';
import { config } from '../../config/index.js';

export function handleAddFile(io, socket) {
  socket.on('add-file', ({ roomCode, file, peerId }, callback) => {
    try {
      if (!checkSocketRateLimit(socket.id)) {
        return safeCallback(callback, { success: false, error: 'Rate limit exceeded' });
      }
      if (!isValidRoomCode(roomCode)) {
        return safeCallback(callback, { success: false, error: 'Invalid room code' });
      }
      if (!isValidFileMetadata(file)) {
        return safeCallback(callback, { success: false, error: 'Invalid file metadata or file too large' });
      }

      const roomFiles = Array.from(files.values()).filter(sharedFile => sharedFile.roomCode === roomCode);
      if (roomFiles.length >= config.MAX_FILES_PER_ROOM) {
        return safeCallback(callback, { success: false, error: `Room file limit (${config.MAX_FILES_PER_ROOM}) reached` });
      }

      const peer = getPeerInRoom(socket, roomCode, peerId);
      const { fileData, transfer } = createTransferRecord({ roomCode, file, peer });

      io.to(roomCode).emit('file-added', mapFileToResponse(fileData));
      io.to(roomCode).emit('transfer-updated', {
        transferId: transfer.id,
        fileId: transfer.fileId,
        status: mapTransferStatus(transfer)
      });

      safeCallback(callback, {
        success: true,
        fileId: fileData.id,
        transferId: transfer.id,
        expiresAt: fileData.expiresAt,
        chunkSize: transfer.chunkSize,
        totalChunks: transfer.totalChunks,
        uploadUrlTemplate: getTransferChunkUrlTemplate(transfer.id)
      });

      log('info', 'Transfer session created', {
        fileName: fileData.name,
        fileId: fileData.id,
        transferId: transfer.id,
        roomCode
      });
    } catch (error) {
      log('error', 'Error in add-file', { error: error.message });
      safeCallback(callback, { success: false, error: error.message });
    }
  });
}