import express from 'express';
import { files } from '../infrastructure/state.js';
import { mapFileToResponse } from '../services/fileService.js';
import { getTransferChunksSummary } from '../services/transferService.js';

export function createTransfersRouter(transferLifecycle) {
  const router = express.Router();

  router.get('/api/transfers/:transferId', (req, res) => {
    try {
      const transfer = transferLifecycle.getTransferOrThrow(req.params.transferId);
      const file = files.get(transfer.fileId);

      res.json({
        transferId: transfer.id,
        file: file ? mapFileToResponse(file) : null,
        state: transfer.state,
        roomCode: transfer.roomCode,
        senderPeerId: transfer.senderPeerId,
        receiverPeerIds: Array.from(transfer.receivers.keys()),
        chunkSize: transfer.chunkSize,
        totalChunks: transfer.totalChunks,
        expiresAt: transfer.expiresAt,
        summary: getTransferChunksSummary(transfer),
        uploadedChunkIndexes: Array.from(transfer.chunkStates.uploaded).sort((a, b) => a - b),
        acknowledgedChunkIndexes: Array.from(transfer.chunkStates.acknowledged).sort((a, b) => a - b)
      });
    } catch (error) {
      res.status(error.message === 'Transfer not found' ? 404 : 400).json({ error: error.message });
    }
  });

  return router;
}