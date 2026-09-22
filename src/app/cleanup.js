import { log } from '../utils/logger.js';
import { config } from '../config/index.js';
import { peers, files, rooms, roomTexts, transfers, socketRateLimits } from '../infrastructure/state.js';

export function startCleanupJobs(io, transferLifecycle) {
  setInterval(() => {
    try {
      const now = Date.now();
      let cleanedFiles = 0;
      let cleanedTransfers = 0;

      for (const [fileId, file] of files) {
        if (file.expiresAt < now) {
          files.delete(fileId);
          cleanedFiles++;
        }
      }

      for (const [transferId, transfer] of transfers) {
        if (transfer.expiresAt < now) {
          transferLifecycle.cancelTransfer(transfer, 'expired');
          transfers.delete(transferId);
          cleanedTransfers++;
        }
      }

      if (cleanedFiles > 0 || cleanedTransfers > 0) {
        log('info', 'Cleanup: Removed expired resources', {
          files: cleanedFiles,
          transfers: cleanedTransfers
        });
      }
    } catch (error) {
      log('error', 'Cleanup error (resources)', { error: error.message });
    }
  }, config.CLEANUP_INTERVAL);

  setInterval(() => {
    try {
      const now = Date.now();

      for (const [socketId, peerInfo] of peers) {
        if (now - peerInfo.lastSeen > config.INACTIVE_TIMEOUT) {
          const roomPeers = rooms.get(peerInfo.roomCode);
          if (roomPeers) {
            const index = roomPeers.findIndex(peer => peer.socketId === socketId);
            if (index > -1) {
              roomPeers.splice(index, 1);
            }
            if (roomPeers.length === 0) {
              rooms.delete(peerInfo.roomCode);
              roomTexts.delete(peerInfo.roomCode);
            }
          }

          for (const transfer of transfers.values()) {
            if (transfer.senderSocketId === socketId) {
              transferLifecycle.cancelTransfer(transfer, 'sender-timeout');
            } else {
              for (const [receiverPeerId, receiver] of transfer.receivers) {
                if (receiver.socketId !== socketId) {
                  continue;
                }
                transferLifecycle.removeReceiverFromTransfer(transfer, receiverPeerId, 'receiver-timeout', false);
                break;
              }
            }
          }

          io.to(peerInfo.roomCode).emit('peer-left', { peerId: peerInfo.id });
          peers.delete(socketId);
          socketRateLimits.delete(socketId);
          log('info', 'Cleanup: Removed inactive peer', { name: peerInfo.name });
        }
      }
    } catch (error) {
      log('error', 'Cleanup error (peers)', { error: error.message });
    }
  }, config.CLEANUP_INTERVAL);
}