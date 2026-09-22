import { log } from '../../utils/logger.js';
import { safeCallback } from '../../utils/safeCallback.js';
import { isValidChunkIndex } from '../../utils/validation.js';
import { checkSocketRateLimit, getPeerInRoom } from '../../services/peerService.js';
import { mapTransferStatus, updateTransferTimestamp } from '../../services/transferService.js';

export function handleAckTransferChunk(io, socket, transferLifecycle) {
  socket.on('ack-transfer-chunk', ({ transferId, chunkIndex, peerId }, callback) => {
    try {
      if (!checkSocketRateLimit(socket.id)) {
        return safeCallback(callback, { success: false, error: 'Rate limit exceeded' });
      }

      const transfer = transferLifecycle.getTransferOrThrow(transferId);
      const peer = getPeerInRoom(socket, transfer.roomCode, peerId);

      if (!isValidChunkIndex(chunkIndex, transfer.totalChunks)) {
        return safeCallback(callback, { success: false, error: 'Invalid chunk index' });
      }

      const receiver = transfer.receivers.get(peer.id);
      if (!receiver || receiver.socketId !== socket.id) {
        return safeCallback(callback, { success: false, error: 'Only the active receiver can acknowledge chunks' });
      }
      if (!transfer.chunkBuffer.has(chunkIndex)) {
        return safeCallback(callback, { success: false, error: 'Chunk not available yet' });
      }
      if (transfer.chunkStates.acknowledgedByPeer.has(peer.id) && transfer.chunkStates.acknowledgedByPeer.get(peer.id).has(chunkIndex)) {
        return safeCallback(callback, {
          success: true,
          state: transfer.state,
          acknowledgedChunks: transfer.chunkStates.acknowledged.size
        });
      }

      const chunkAcknowledgements = transfer.chunkStates.chunkAcknowledgements.get(chunkIndex) || new Set();
      chunkAcknowledgements.add(peer.id);
      transfer.chunkStates.chunkAcknowledgements.set(chunkIndex, chunkAcknowledgements);

      const receiverAcknowledgements = transfer.chunkStates.acknowledgedByPeer.get(peer.id) || new Set();
      receiverAcknowledgements.add(chunkIndex);
      transfer.chunkStates.acknowledgedByPeer.set(peer.id, receiverAcknowledgements);

      if (chunkAcknowledgements.size >= transfer.receivers.size) {
        transfer.chunkStates.acknowledged.add(chunkIndex);
        transfer.chunkBuffer.delete(chunkIndex);
      }
      const receiverCompleted = receiverAcknowledgements.size === transfer.totalChunks;
      transfer.state = transfer.receivers.size > 0 ? 'transferring' : transfer.chunkStates.uploaded.size > 0 ? 'ready' : 'pending';
      updateTransferTimestamp(transfer);

      io.to(transfer.roomCode).emit('transfer-updated', {
        transferId,
        fileId: transfer.fileId,
        status: mapTransferStatus(transfer),
        chunkIndex
      });

      if (receiverCompleted) {
        io.to(transfer.roomCode).emit('transfer-completed', {
          transferId,
          fileId: transfer.fileId,
          peerId: peer.id,
          status: mapTransferStatus(transfer)
        });

        transferLifecycle.removeReceiverFromTransfer(transfer, peer.id, 'receiver-completed', false);
      }

      safeCallback(callback, {
        success: true,
        state: transfer.state,
        acknowledgedChunks: transfer.chunkStates.acknowledged.size
      });
    } catch (error) {
      log('error', 'Error in ack-transfer-chunk', { error: error.message });
      safeCallback(callback, { success: false, error: error.message });
    }
  });
}