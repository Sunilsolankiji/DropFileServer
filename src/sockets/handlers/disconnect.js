import { log } from '../../utils/logger.js';
import { peers, rooms, roomTexts, transfers, socketRateLimits } from '../../infrastructure/state.js';

export function handleDisconnect(io, socket, transferLifecycle) {
  socket.on('disconnect', () => {
    const peerInfo = peers.get(socket.id);
    socketRateLimits.delete(socket.id);

    if (peerInfo) {
      const { roomCode, name, id } = peerInfo;
      const roomPeers = rooms.get(roomCode);

      if (roomPeers) {
        const index = roomPeers.findIndex(peer => peer.socketId === socket.id);
        if (index > -1) {
          roomPeers.splice(index, 1);
        }

        if (roomPeers.length === 0) {
          rooms.delete(roomCode);
          roomTexts.delete(roomCode);
          log('info', 'Room cleaned up (empty)', { roomCode });
        }
      }

      for (const transfer of transfers.values()) {
        if (transfer.senderSocketId === socket.id) {
          transferLifecycle.cancelTransfer(transfer, 'sender-offline');
        } else {
          for (const [receiverPeerId, receiver] of transfer.receivers) {
            if (receiver.socketId !== socket.id) {
              continue;
            }
            transferLifecycle.removeReceiverFromTransfer(transfer, receiverPeerId, 'receiver-offline');
            break;
          }
        }
      }

      io.to(roomCode).emit('peer-left', { peerId: id });
      peers.delete(socket.id);

      log('info', 'Peer disconnected', { name, roomCode });
    }
  });
}