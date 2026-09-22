import { log } from '../../utils/logger.js';
import { safeCallback } from '../../utils/safeCallback.js';
import { isValidRoomCode } from '../../utils/validation.js';
import { checkSocketRateLimit, getPeerInRoom } from '../../services/peerService.js';

export function handleCancelTransfer(socket, transferLifecycle) {
  socket.on('cancel-transfer', ({ transferId, roomCode, peerId, reason }, callback) => {
    try {
      if (!checkSocketRateLimit(socket.id)) {
        return safeCallback(callback, { success: false, error: 'Rate limit exceeded' });
      }

      if (!isValidRoomCode(roomCode)) {
        return safeCallback(callback, { success: false, error: 'Invalid room code' });
      }

      const transfer = transferLifecycle.getTransferOrThrow(transferId);
      const peer = getPeerInRoom(socket, roomCode, peerId);
      const isSender = transfer.senderPeerId === peer.id;
      const isReceiver = transfer.receivers.has(peer.id);

      if (!isSender && !isReceiver) {
        return safeCallback(callback, { success: false, error: 'Unauthorized transfer cancellation' });
      }

      if (isSender) {
        transferLifecycle.cancelTransfer(transfer, reason || 'cancelled');
      } else {
        transferLifecycle.removeReceiverFromTransfer(transfer, peer.id, reason || 'receiver-cancelled');
      }
      safeCallback(callback, { success: true });
    } catch (error) {
      log('error', 'Error in cancel-transfer', { error: error.message });
      safeCallback(callback, { success: false, error: error.message });
    }
  });
}