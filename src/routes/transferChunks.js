import express from 'express';
import { log } from '../utils/logger.js';
import { parsePositiveInteger, isValidChunkIndex } from '../utils/validation.js';
import { canAcceptChunk, mapTransferStatus, updateTransferTimestamp } from '../services/transferService.js';
import { files } from '../infrastructure/state.js';

export function createTransferChunksRouter(io, transferLifecycle) {
  const router = express.Router();

  router.post('/api/transfers/:transferId/chunks/:chunkIndex', (req, res) => {
    try {
      const { transferId } = req.params;
      const chunkIndex = parsePositiveInteger(req.params.chunkIndex);
      const senderPeerId = req.header('x-peer-id');

      if (chunkIndex === null) {
        return res.status(400).json({ error: 'Invalid chunk index' });
      }

      const transfer = transferLifecycle.getTransferOrThrow(transferId);
      if (!isValidChunkIndex(chunkIndex, transfer.totalChunks)) {
        return res.status(400).json({ error: 'Chunk index out of range' });
      }
      if (!senderPeerId || senderPeerId !== transfer.senderPeerId) {
        return res.status(403).json({ error: 'Only the sender can upload chunks' });
      }
      if (!Buffer.isBuffer(req.body) || req.body.length === 0) {
        return res.status(400).json({ error: 'Chunk payload is required' });
      }
      if (req.body.length > transfer.chunkSize || (chunkIndex < transfer.totalChunks - 1 && req.body.length !== transfer.chunkSize)) {
        return res.status(400).json({ error: 'Chunk size does not match transfer contract' });
      }
      if (!canAcceptChunk(transfer, chunkIndex)) {
        return res.status(409).json({ error: 'Transfer buffer full or chunk already uploaded' });
      }

      transfer.chunkBuffer.set(chunkIndex, {
        data: req.body,
        uploadedAt: Date.now(),
        size: req.body.length,
        hash: req.header('x-chunk-hash') || null
      });
      transfer.chunkStates.uploaded.add(chunkIndex);
      transfer.state = transfer.receivers.size > 0 ? 'transferring' : 'ready';
      updateTransferTimestamp(transfer);

      const file = files.get(transfer.fileId);
      if (file) {
        file.status = transfer.state;
      }

      io.to(transfer.roomCode).emit('transfer-updated', {
        transferId,
        fileId: transfer.fileId,
        status: mapTransferStatus(transfer),
        chunkIndex
      });

      res.status(202).json({
        success: true,
        transferId,
        chunkIndex,
        state: transfer.state
      });
    } catch (error) {
      log('error', 'Error in chunk upload', { error: error.message });
      res.status(error.message === 'Transfer not found' ? 404 : 400).json({ error: error.message });
    }
  });

  router.get('/api/transfers/:transferId/chunks/:chunkIndex', (req, res) => {
    try {
      const { transferId } = req.params;
      const chunkIndex = parsePositiveInteger(req.params.chunkIndex);
      const receiverPeerId = req.header('x-peer-id');

      if (chunkIndex === null) {
        return res.status(400).json({ error: 'Invalid chunk index' });
      }

      const transfer = transferLifecycle.getTransferOrThrow(transferId);
      if (!isValidChunkIndex(chunkIndex, transfer.totalChunks)) {
        return res.status(400).json({ error: 'Chunk index out of range' });
      }
      const receiver = receiverPeerId ? transfer.receivers.get(receiverPeerId) : null;
      if (!receiver) {
        return res.status(403).json({ error: 'Only active receivers can fetch chunks' });
      }

      receiver.lastSeen = Date.now();

      const chunk = transfer.chunkBuffer.get(chunkIndex);
      if (!chunk) {
        return res.status(404).json({ error: 'Chunk not available yet' });
      }

      updateTransferTimestamp(transfer);
      res.setHeader('Content-Type', 'application/octet-stream');
      res.setHeader('Content-Length', chunk.size);
      res.setHeader('X-Chunk-Index', chunkIndex.toString());
      if (chunk.hash) {
        res.setHeader('X-Chunk-Hash', chunk.hash);
      }
      res.status(200).send(chunk.data);
    } catch (error) {
      log('error', 'Error in chunk fetch', { error: error.message });
      res.status(error.message === 'Transfer not found' ? 404 : 400).json({ error: error.message });
    }
  });

  return router;
}