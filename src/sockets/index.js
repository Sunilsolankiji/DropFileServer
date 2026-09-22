import { log } from '../utils/logger.js';
import { handleJoinRoom } from './handlers/joinRoom.js';
import { handleAddFile } from './handlers/addFile.js';
import { handleStartTransfer } from './handlers/startTransfer.js';
import { handleDownloadFile } from './handlers/downloadFile.js';
import { handleGetTransferState } from './handlers/getTransferState.js';
import { handleAckTransferChunk } from './handlers/ackTransferChunk.js';
import { handleCancelTransfer } from './handlers/cancelTransfer.js';
import { handleRemoveFile } from './handlers/removeFile.js';
import { handleAddText } from './handlers/addText.js';
import { handleUpdatePeerName } from './handlers/updatePeerName.js';
import { handleHeartbeat } from './handlers/heartbeat.js';
import { handleDisconnect } from './handlers/disconnect.js';

export function registerSocketHandlers(io, transferLifecycle) {
  io.on('connection', (socket) => {
    log('info', 'Peer connected', { socketId: socket.id });

    handleJoinRoom(socket);
    handleAddFile(io, socket);
    handleStartTransfer(io, socket, transferLifecycle);
    handleDownloadFile(socket, transferLifecycle);
    handleGetTransferState(socket, transferLifecycle);
    handleAckTransferChunk(io, socket, transferLifecycle);
    handleCancelTransfer(socket, transferLifecycle);
    handleRemoveFile(io, socket, transferLifecycle);
    handleAddText(socket);
    handleUpdatePeerName(io, socket);
    handleHeartbeat(socket);
    handleDisconnect(io, socket, transferLifecycle);
  });
}