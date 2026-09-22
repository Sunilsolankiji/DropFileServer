import express from 'express';
import http from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import helmet from 'helmet';
import rateLimit from 'express-rate-limit';
import { v4 as uuidv4 } from 'uuid';
import os from 'os';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
const server = http.createServer(app);

const PORT = parseInt(process.env.PORT, 10) || 3001;
const NODE_ENV = process.env.NODE_ENV || 'development';
const ALLOWED_ORIGINS = process.env.ALLOWED_ORIGINS?.split(',').map(origin => origin.trim()).filter(Boolean) || ['http://localhost:3000', 'http://localhost:9002', 'https://sunilsolankiji.github.io'];
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL?.trim().replace(/\/+$/, '') || '';
const MAX_FILE_SIZE = parseInt(process.env.MAX_FILE_SIZE, 10) || 2 * 1024 * 1024 * 1024;
const FILE_TTL_MS = parseInt(process.env.FILE_TTL_MS, 10) || 60 * 60 * 1000;
const CLEANUP_INTERVAL = parseInt(process.env.CLEANUP_INTERVAL, 10) || 30000;
const MAX_FILES_PER_ROOM = parseInt(process.env.MAX_FILES_PER_ROOM, 10) || 50;
const INACTIVE_TIMEOUT = parseInt(process.env.INACTIVE_TIMEOUT, 10) || 30000;
const DEFAULT_CHUNK_SIZE = parseInt(process.env.CHUNK_SIZE, 10) || 1024 * 1024;
const MAX_CHUNK_SIZE = parseInt(process.env.MAX_CHUNK_SIZE, 10) || 4 * 1024 * 1024;
const MAX_INFLIGHT_CHUNKS = parseInt(process.env.MAX_INFLIGHT_CHUNKS, 10) || 8;
const MAX_TRANSFER_BUFFER_BYTES = parseInt(process.env.MAX_TRANSFER_BUFFER_BYTES, 10) || 64 * 1024 * 1024;

const corsOptions = {
    origin: NODE_ENV === 'development' ? '*' : ALLOWED_ORIGINS,
    methods: ['GET', 'POST', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'x-peer-id', 'x-chunk-hash'],
    exposedHeaders: ['Content-Length', 'X-Chunk-Index', 'X-Chunk-Hash']
};

const io = new Server(server, {
    cors: corsOptions, maxHttpBufferSize: MAX_CHUNK_SIZE + 64 * 1024
});

app.use(helmet({
    crossOriginResourcePolicy: { policy: 'cross-origin' }
}));

const apiLimiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 300,
    skip: req => req.method === 'OPTIONS' || req.path.startsWith('/api/transfers/') && /\/chunks\/\d+$/.test(req.path),
    message: { error: 'Too many requests, please try again later.' }
});

app.use(cors(corsOptions));
app.options('*', cors(corsOptions));
app.use('/api/', apiLimiter);
app.use(express.json({ limit: '2mb' }));
app.use(express.urlencoded({ limit: '2mb', extended: true }));
app.use('/api/transfers/:transferId/chunks/:chunkIndex', express.raw({
    type: '*/*', limit: `${MAX_CHUNK_SIZE}b`
}));

const peers = new Map();
const files = new Map();
const rooms = new Map();
const roomTexts = new Map();
const transfers = new Map();
const socketRateLimits = new Map();

const SOCKET_RATE_LIMIT = 60;
const SOCKET_RATE_WINDOW = 1000;

function log(level, message, data = {}) {
    const timestamp = new Date().toISOString();
    const dataStr = Object.keys(data).length ? ` ${JSON.stringify(data)}` : '';
    console.log(`[${timestamp}] [${level.toUpperCase()}] ${message}${dataStr}`);
}

function isValidRoomCode(roomCode) {
    if (!roomCode || typeof roomCode !== 'string') return false;
    return /^[a-zA-Z0-9_-]{3,50}$/.test(roomCode);
}

function isValidPeerName(name) {
    if (!name || typeof name !== 'string') return false;
    return name.length >= 1 && name.length <= 50 && !/<[^>]*>/.test(name);
}

function isValidFileMetadata(file) {
    if (!file || typeof file !== 'object') return false;
    if (!file.name || typeof file.name !== 'string' || file.name.length > 255) return false;
    if (typeof file.size !== 'number' || file.size <= 0 || file.size > MAX_FILE_SIZE) return false;
    if (file.type && typeof file.type !== 'string') return false;
    return true;
}

function isValidTextMessage(message) {
    if (!message || typeof message !== 'object') return false;
    if (message.peerName && !isValidPeerName(message.peerName)) return false;

    const messageText = typeof message.text === 'string' ? message.text : typeof message.message === 'string' ? message.message : null;

    if (messageText === null) return false;

    const trimmedText = messageText.trim();
    return trimmedText.length >= 1 && trimmedText.length <= 2000;
}

function mapTextToResponse(message) {
    return {
        id: message.id,
        text: message.text,
        message: message.message,
        peerId: message.peerId,
        peerName: message.peerName,
        createdAt: message.createdAt
    };
}

function getRoomTexts(roomCode) {
    return roomTexts.get(roomCode) || [];
}

function checkSocketRateLimit(socketId) {
    const now = Date.now();
    let rateInfo = socketRateLimits.get(socketId);

    if (!rateInfo || now > rateInfo.resetTime) {
        rateInfo = { count: 0, resetTime: now + SOCKET_RATE_WINDOW };
        socketRateLimits.set(socketId, rateInfo);
    }

    rateInfo.count++;
    return rateInfo.count <= SOCKET_RATE_LIMIT;
}

function safeCallback(callback, response) {
    if (typeof callback === 'function') {
        try {
            callback(response);
        } catch (err) {
            log('error', 'Callback error', { error: err.message });
        }
    }
}

function mapTransferStatus(transfer) {
    const completedReceiverPeerIds = [];
    for (const [peerId, acknowledgedChunks] of transfer.chunkStates.acknowledgedByPeer) {
        if (acknowledgedChunks.size === transfer.totalChunks) {
            completedReceiverPeerIds.push(peerId);
        }
    }

    return {
        state: transfer.state,
        uploadedChunks: transfer.chunkStates.uploaded.size,
        acknowledgedChunks: transfer.chunkStates.acknowledged.size,
        activeReceivers: transfer.receivers.size,
        receiverPeerIds: Array.from(transfer.receivers.keys()),
        completedReceiverPeerIds,
        senderConnected: Boolean(transfer.senderSocketId && peers.has(transfer.senderSocketId))
    };
}

function mapFileToResponse(file) {
    return {
        id: file.id,
        name: file.name,
        size: file.size,
        type: file.type,
        peerId: file.peerId,
        peerName: file.peerName,
        roomCode: file.roomCode,
        expiresAt: file.expiresAt,
        uploadedAt: file.uploadedAt,
        transferId: file.transferId,
        totalChunks: file.totalChunks,
        chunkSize: file.chunkSize,
        status: file.status,
        hash: file.hash,
        transfer: file.transferId ? mapTransferStatus(transfers.get(file.transferId) || {
            state: 'expired', chunkStates: {
                uploaded: new Set(),
                acknowledged: new Set(),
                acknowledgedByPeer: new Map(),
                chunkAcknowledgements: new Map()
            }, receivers: new Map(), senderSocketId: null
        }) : undefined
    };
}

function getLocalIP() {
    const interfaces = os.networkInterfaces();
    for (const name of Object.keys(interfaces)) {
        for (const iface of interfaces[name]) {
            if (iface.family === 'IPv4' && !iface.internal) {
                return iface.address;
            }
        }
    }
    return 'localhost';
}

const LOCAL_IP = getLocalIP();

function getPublicBaseUrl() {
    return PUBLIC_BASE_URL || `http://${LOCAL_IP}:${PORT}`;
}

function getTransferChunkUrlTemplate(transferId) {
    const path = `/api/transfers/${transferId}/chunks/{chunkIndex}`;
    return PUBLIC_BASE_URL ? `${PUBLIC_BASE_URL}${path}` : path;
}

function getServerInfo() {
    return {
        ip: LOCAL_IP, port: PORT, publicBaseUrl: getPublicBaseUrl()
    };
}

function getRoomFiles(roomCode) {
    return Array.from(files.values())
        .filter(file => file.roomCode === roomCode)
        .map(mapFileToResponse);
}

function getPeerBySocket(socketId) {
    return peers.get(socketId) || null;
}

function getPeerInRoom(socket, roomCode, peerId) {
    const currentPeer = getPeerBySocket(socket.id);
    if (!currentPeer) {
        throw new Error('Peer not registered');
    }
    if (currentPeer.roomCode !== roomCode) {
        throw new Error('Peer not registered in this room');
    }
    if (peerId && currentPeer.id !== peerId) {
        throw new Error('Peer ID does not match current connection');
    }
    return currentPeer;
}

function parsePositiveInteger(value) {
    const parsed = Number.parseInt(value, 10);
    return Number.isInteger(parsed) && parsed >= 0 ? parsed : null;
}

function normalizeChunkSize(chunkSize) {
    const requested = parsePositiveInteger(chunkSize);
    if (!requested || requested <= 0) {
        return DEFAULT_CHUNK_SIZE;
    }
    return Math.min(requested, MAX_CHUNK_SIZE);
}

function isValidChunkIndex(index, totalChunks) {
    return Number.isInteger(index) && index >= 0 && index < totalChunks;
}

function getBufferedBytes(transfer) {
    let total = 0;
    for (const chunkInfo of transfer.chunkBuffer.values()) {
        total += chunkInfo.data.length;
    }
    return total;
}

function isTransferExpired(transfer) {
    return transfer.expiresAt < Date.now();
}

function cancelTransfer(transfer, reason, notifyRoom = true) {
    if (!transfer || transfer.state === 'completed' || transfer.state === 'cancelled' || transfer.state === 'expired') {
        return;
    }

    transfer.state = reason === 'expired' ? 'expired' : 'cancelled';
    transfer.cancelReason = reason;
    transfer.chunkBuffer.clear();

    const file = files.get(transfer.fileId);
    if (file) {
        file.status = transfer.state;
    }

    if (notifyRoom) {
        io.to(transfer.roomCode).emit('transfer-updated', {
            transferId: transfer.id, fileId: transfer.fileId, status: mapTransferStatus(transfer), reason
        });
    }
}

function completeTransfer(transfer) {
    transfer.state = 'completed';

    const file = files.get(transfer.fileId);
    if (file) {
        file.status = 'completed';
    }

    io.to(transfer.roomCode).emit('transfer-completed', {
        transferId: transfer.id, fileId: transfer.fileId, status: mapTransferStatus(transfer)
    });
}

function removeReceiverFromTransfer(transfer, receiverPeerId, reason, notifyRoom = true) {
    if (!transfer.receivers.has(receiverPeerId)) {
        return false;
    }

    transfer.receivers.delete(receiverPeerId);
    transfer.chunkStates.acknowledgedByPeer.delete(receiverPeerId);

    for (const [chunkIndex, acknowledgedPeers] of transfer.chunkStates.chunkAcknowledgements) {
        acknowledgedPeers.delete(receiverPeerId);
        if (acknowledgedPeers.size === 0) {
            transfer.chunkStates.chunkAcknowledgements.delete(chunkIndex);
        }
        if (acknowledgedPeers.size < transfer.receivers.size) {
            transfer.chunkStates.acknowledged.delete(chunkIndex);
        }
        if (acknowledgedPeers.size >= transfer.receivers.size && transfer.chunkStates.uploaded.has(chunkIndex)) {
            transfer.chunkBuffer.delete(chunkIndex);
        }
    }

    transfer.state = transfer.receivers.size > 0 ? 'transferring' : transfer.chunkStates.uploaded.size > 0 ? 'ready' : 'pending';
    if (transfer.receivers.size === 0 && transfer.chunkStates.acknowledged.size === transfer.totalChunks) {
        completeTransfer(transfer);
    }
    updateTransferTimestamp(transfer);

    const file = files.get(transfer.fileId);
    if (file) {
        file.status = transfer.state;
    }

    if (notifyRoom) {
        io.to(transfer.roomCode).emit('transfer-updated', {
            transferId: transfer.id,
            fileId: transfer.fileId,
            status: mapTransferStatus(transfer),
            reason,
            peerId: receiverPeerId
        });
    }

    return true;
}

function canAcceptChunk(transfer, chunkIndex) {
    if (transfer.chunkBuffer.has(chunkIndex)) {
        return false;
    }

    if (transfer.chunkBuffer.size >= MAX_INFLIGHT_CHUNKS) {
        return false;
    }

    return getBufferedBytes(transfer) < MAX_TRANSFER_BUFFER_BYTES;
}

function getTransferOrThrow(transferId) {
    const transfer = transfers.get(transferId);
    if (!transfer) {
        throw new Error('Transfer not found');
    }
    if (isTransferExpired(transfer)) {
        cancelTransfer(transfer, 'expired');
        throw new Error('Transfer has expired');
    }
    return transfer;
}

function getFileOrThrow(fileId) {
    const file = files.get(fileId);
    if (!file) {
        throw new Error('File not found or expired');
    }
    if (file.expiresAt < Date.now()) {
        files.delete(fileId);
        if (file.transferId) {
            transfers.delete(file.transferId);
        }
        throw new Error('File has expired');
    }
    return file;
}

function createTransferRecord({ roomCode, file, peer }) {
    const fileId = file.id || uuidv4();
    const transferId = uuidv4();
    const chunkSize = normalizeChunkSize(file.chunkSize);
    const totalChunks = file.totalChunks && Number.isInteger(file.totalChunks) ? file.totalChunks : Math.ceil(file.size / chunkSize);
    const now = Date.now();
    const expiresAt = now + FILE_TTL_MS;

    if (!Number.isInteger(totalChunks) || totalChunks <= 0) {
        throw new Error('Invalid total chunks');
    }

    const fileData = {
        id: fileId,
        name: file.name.substring(0, 255),
        size: file.size,
        type: file.type || 'application/octet-stream',
        peerId: peer.id,
        peerName: peer.name,
        roomCode,
        expiresAt,
        uploadedAt: now,
        ownerSocketId: peer.socketId,
        transferId,
        totalChunks,
        chunkSize,
        hash: typeof file.hash === 'string' ? file.hash : null,
        status: 'pending'
    };

    const transfer = {
        id: transferId,
        fileId,
        roomCode,
        senderPeerId: peer.id,
        senderSocketId: peer.socketId,
        senderName: peer.name,
        fileName: fileData.name,
        fileSize: fileData.size,
        mimeType: fileData.type,
        hash: fileData.hash,
        chunkSize,
        totalChunks,
        state: 'pending',
        createdAt: now,
        updatedAt: now,
        expiresAt,
        chunkStates: {
            uploaded: new Set(),
            acknowledged: new Set(),
            acknowledgedByPeer: new Map(),
            chunkAcknowledgements: new Map()
        },
        chunkBuffer: new Map(),
        chunkUploaders: new Set(),
        receivers: new Map()
    };

    files.set(fileId, fileData);
    transfers.set(transferId, transfer);
    return { fileData, transfer };
}

function updateTransferTimestamp(transfer) {
    transfer.updatedAt = Date.now();
}

function getTransferChunksSummary(transfer) {
    return {
        totalChunks: transfer.totalChunks,
        uploadedChunks: Array.from(transfer.chunkStates.uploaded).sort((a, b) => a - b),
        acknowledgedChunks: Array.from(transfer.chunkStates.acknowledged).sort((a, b) => a - b)
    };
}

io.on('connection', (socket) => {
    log('info', 'Peer connected', { socketId: socket.id });

    socket.on('join-room', ({ roomCode, peerName, peerId }, callback) => {
        try {
            if (!checkSocketRateLimit(socket.id)) {
                return safeCallback(callback, { success: false, error: 'Rate limit exceeded' });
            }

            if (!isValidRoomCode(roomCode)) {
                return safeCallback(callback, { success: false, error: 'Invalid room code format' });
            }
            if (peerName && !isValidPeerName(peerName)) {
                return safeCallback(callback, { success: false, error: 'Invalid peer name' });
            }
            if (!peerId || typeof peerId !== 'string') {
                return safeCallback(callback, { success: false, error: 'Invalid peer ID' });
            }

            if (peers.has(socket.id)) {
                const existingPeer = peers.get(socket.id);
                log('warn', 'Duplicate join-room attempt', { socketId: socket.id, existingName: existingPeer.name });

                const roomPeers = rooms.get(roomCode) || [];
                const otherPeers = roomPeers.filter(peer => peer.socketId !== socket.id);
                const texts = getRoomTexts(roomCode)
                    .slice()
                    .sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt))
                    .map(mapTextToResponse);

                return safeCallback(callback, {
                    success: true, peers: otherPeers, files: getRoomFiles(roomCode), texts, serverInfo: getServerInfo()
                });
            }

            socket.join(roomCode);

            if (!rooms.has(roomCode)) {
                rooms.set(roomCode, []);
            }
            if (!roomTexts.has(roomCode)) {
                roomTexts.set(roomCode, []);
            }

            const sanitizedName = peerName ? peerName.trim().substring(0, 50) : `Device ${peerId.slice(0, 6)}`;
            const peerInfo = {
                id: peerId,
                socketId: socket.id,
                name: sanitizedName,
                joinedAt: Date.now(),
                lastSeen: Date.now(),
                ip: socket.handshake.address,
                isActive: true
            };

            const roomPeers = rooms.get(roomCode);
            const existingPeerIndex = roomPeers.findIndex(peer => peer.id === peerId);
            const isNewPeer = existingPeerIndex === -1;

            if (existingPeerIndex > -1) {
                const oldSocketId = roomPeers[existingPeerIndex].socketId;
                if (oldSocketId !== socket.id) {
                    peers.delete(oldSocketId);
                }
                roomPeers[existingPeerIndex] = peerInfo;
            } else {
                roomPeers.push(peerInfo);
            }

            peers.set(socket.id, { ...peerInfo, roomCode });

            if (isNewPeer) {
                socket.to(roomCode).emit('peer-joined', peerInfo);
            }

            const otherPeers = roomPeers.filter(peer => peer.socketId !== socket.id);
            const texts = getRoomTexts(roomCode)
                .slice()
                .sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt))
                .map(mapTextToResponse);

            safeCallback(callback, {
                success: true, peers: otherPeers, files: getRoomFiles(roomCode), texts, serverInfo: getServerInfo()
            });

            log('info', 'Peer joined room', {
                name: peerInfo.name, roomCode, isNew: isNewPeer, roomPeers: roomPeers.length
            });
        } catch (error) {
            log('error', 'Error in join-room', { error: error.message });
            safeCallback(callback, { success: false, error: error.message });
        }
    });

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
            if (roomFiles.length >= MAX_FILES_PER_ROOM) {
                return safeCallback(callback, {
                    success: false, error: `Room file limit (${MAX_FILES_PER_ROOM}) reached`
                });
            }

            const peer = getPeerInRoom(socket, roomCode, peerId);
            const { fileData, transfer } = createTransferRecord({ roomCode, file, peer });

            io.to(roomCode).emit('file-added', mapFileToResponse(fileData));
            io.to(roomCode).emit('transfer-updated', {
                transferId: transfer.id, fileId: transfer.fileId, status: mapTransferStatus(transfer)
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
                fileName: fileData.name, fileId: fileData.id, transferId: transfer.id, roomCode
            });
        } catch (error) {
            log('error', 'Error in add-file', { error: error.message });
            safeCallback(callback, { success: false, error: error.message });
        }
    });

    socket.on('start-transfer', ({ roomCode, fileId, peerId }, callback) => {
        try {
            if (!checkSocketRateLimit(socket.id)) {
                return safeCallback(callback, { success: false, error: 'Rate limit exceeded' });
            }

            if (!isValidRoomCode(roomCode)) {
                return safeCallback(callback, { success: false, error: 'Invalid room code' });
            }

            const peer = getPeerInRoom(socket, roomCode, peerId);
            const file = getFileOrThrow(fileId);

            if (file.roomCode !== roomCode) {
                return safeCallback(callback, { success: false, error: 'File not in this room' });
            }

            const transfer = getTransferOrThrow(file.transferId);
            if (transfer.senderPeerId === peer.id) {
                return safeCallback(callback, { success: false, error: 'Sender cannot start download for own file' });
            }
            transfer.receivers.set(peer.id, {
                peerId: peer.id, socketId: socket.id, joinedAt: Date.now(), lastSeen: Date.now()
            });
            transfer.state = 'transferring';
            updateTransferTimestamp(transfer);

            const fileRecord = files.get(fileId);
            if (fileRecord) {
                fileRecord.status = transfer.state;
            }

            io.to(roomCode).emit('transfer-updated', {
                transferId: transfer.id, fileId, status: mapTransferStatus(transfer)
            });

            safeCallback(callback, {
                success: true,
                transferId: transfer.id,
                fileId,
                chunkSize: transfer.chunkSize,
                totalChunks: transfer.totalChunks,
                state: transfer.state,
                downloadUrlTemplate: getTransferChunkUrlTemplate(transfer.id),
                uploadedChunkIndexes: Array.from(transfer.chunkStates.uploaded).sort((a, b) => a - b),
                acknowledgedChunkIndexes: Array.from(transfer.chunkStates.acknowledged).sort((a, b) => a - b),
                activeReceivers: transfer.receivers.size
            });
        } catch (error) {
            log('error', 'Error in start-transfer', { error: error.message });
            safeCallback(callback, { success: false, error: error.message });
        }
    });

    socket.on('download-file', ({ fileId, roomCode, peerId }, callback) => {
        try {
            const file = getFileOrThrow(fileId);
            const transfer = getTransferOrThrow(file.transferId);
            if (!peerId || typeof peerId !== 'string') {
                return safeCallback(callback, { success: false, error: 'Invalid peer ID' });
            }
            return safeCallback(callback, {
                success: true, file: mapFileToResponse(file), transfer: {
                    transferId: transfer.id,
                    chunkSize: transfer.chunkSize,
                    totalChunks: transfer.totalChunks,
                    state: transfer.state,
                    roomCode: file.roomCode,
                    startRequired: !transfer.receivers.has(peerId),
                    downloadUrlTemplate: getTransferChunkUrlTemplate(transfer.id),
                    uploadedChunkIndexes: Array.from(transfer.chunkStates.uploaded).sort((a, b) => a - b),
                    acknowledgedChunkIndexes: Array.from(transfer.chunkStates.acknowledged).sort((a, b) => a - b)
                }
            });
        } catch (error) {
            log('error', 'Error in download-file', { error: error.message });
            safeCallback(callback, { success: false, error: error.message });
        }
    });

    socket.on('get-transfer-state', ({ transferId }, callback) => {
        try {
            if (!checkSocketRateLimit(socket.id)) {
                return safeCallback(callback, { success: false, error: 'Rate limit exceeded' });
            }

            const transfer = getTransferOrThrow(transferId);
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

    socket.on('ack-transfer-chunk', ({ transferId, chunkIndex, peerId }, callback) => {
        try {
            if (!checkSocketRateLimit(socket.id)) {
                return safeCallback(callback, { success: false, error: 'Rate limit exceeded' });
            }

            const transfer = getTransferOrThrow(transferId);
            const peer = getPeerInRoom(socket, transfer.roomCode, peerId);

            if (!isValidChunkIndex(chunkIndex, transfer.totalChunks)) {
                return safeCallback(callback, { success: false, error: 'Invalid chunk index' });
            }

            const receiver = transfer.receivers.get(peer.id);
            if (!receiver || receiver.socketId !== socket.id) {
                return safeCallback(callback, {
                    success: false, error: 'Only the active receiver can acknowledge chunks'
                });
            }
            if (!transfer.chunkBuffer.has(chunkIndex)) {
                return safeCallback(callback, { success: false, error: 'Chunk not available yet' });
            }
            if (transfer.chunkStates.acknowledgedByPeer.has(peer.id) && transfer.chunkStates.acknowledgedByPeer.get(peer.id).has(chunkIndex)) {
                return safeCallback(callback, {
                    success: true, state: transfer.state, acknowledgedChunks: transfer.chunkStates.acknowledged.size
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
                transferId, fileId: transfer.fileId, status: mapTransferStatus(transfer), chunkIndex
            });

            if (receiverCompleted) {
                io.to(transfer.roomCode).emit('transfer-completed', {
                    transferId, fileId: transfer.fileId, peerId: peer.id, status: mapTransferStatus(transfer)
                });

                removeReceiverFromTransfer(transfer, peer.id, 'receiver-completed', false);
            }

            safeCallback(callback, {
                success: true, state: transfer.state, acknowledgedChunks: transfer.chunkStates.acknowledged.size
            });
        } catch (error) {
            log('error', 'Error in ack-transfer-chunk', { error: error.message });
            safeCallback(callback, { success: false, error: error.message });
        }
    });

    socket.on('cancel-transfer', ({ transferId, roomCode, peerId, reason }, callback) => {
        try {
            if (!checkSocketRateLimit(socket.id)) {
                return safeCallback(callback, { success: false, error: 'Rate limit exceeded' });
            }

            if (!isValidRoomCode(roomCode)) {
                return safeCallback(callback, { success: false, error: 'Invalid room code' });
            }

            const transfer = getTransferOrThrow(transferId);
            const peer = getPeerInRoom(socket, roomCode, peerId);
            const isSender = transfer.senderPeerId === peer.id;
            const isReceiver = transfer.receivers.has(peer.id);

            if (!isSender && !isReceiver) {
                return safeCallback(callback, { success: false, error: 'Unauthorized transfer cancellation' });
            }

            if (isSender) {
                cancelTransfer(transfer, reason || 'cancelled');
            } else {
                removeReceiverFromTransfer(transfer, peer.id, reason || 'receiver-cancelled');
            }
            safeCallback(callback, { success: true });
        } catch (error) {
            log('error', 'Error in cancel-transfer', { error: error.message });
            safeCallback(callback, { success: false, error: error.message });
        }
    });

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
                cancelTransfer(transfers.get(file.transferId), 'removed');
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

    socket.on('add-text', ({ roomCode, text }, callback) => {
        try {
            if (!checkSocketRateLimit(socket.id)) {
                return safeCallback(callback, { success: false, error: 'Rate limit exceeded' });
            }

            if (!isValidRoomCode(roomCode)) {
                return safeCallback(callback, { success: false, error: 'Invalid room code' });
            }
            if (!isValidTextMessage(text)) {
                return safeCallback(callback, { success: false, error: 'Invalid text message' });
            }

            const roomPeers = rooms.get(roomCode);
            if (!roomPeers) {
                return safeCallback(callback, { success: false, error: 'Room not found' });
            }

            const currentPeer = peers.get(socket.id);
            if (!currentPeer || currentPeer.roomCode !== roomCode || currentPeer.id !== text.peerId) {
                return safeCallback(callback, { success: false, error: 'Peer not registered in this room' });
            }

            const messageContent = typeof text.text === 'string' ? text.text : text.message;
            const message = {
                id: typeof text.id === 'string' && text.id ? text.id : uuidv4(),
                roomCode,
                text: messageContent.trim(),
                message: messageContent.trim(),
                peerId: currentPeer.id,
                peerName: currentPeer.name,
                createdAt: text.createdAt && !Number.isNaN(Date.parse(text.createdAt)) ? text.createdAt : new Date().toISOString(),
                ownerSocketId: socket.id
            };

            const messages = getRoomTexts(roomCode);
            messages.push(message);
            roomTexts.set(roomCode, messages);

            socket.to(roomCode).emit('text-added', mapTextToResponse(message));

            safeCallback(callback, {
                success: true, message: mapTextToResponse(message)
            });

            log('info', 'Text added', { messageId: message.id, peerName: message.peerName, roomCode });
        } catch (error) {
            log('error', 'Error in add-text', { error: error.message });
            safeCallback(callback, { success: false, error: error.message });
        }
    });

    socket.on('update-peer-name', ({ roomCode, peerId, peerName }, callback) => {
        try {
            if (!checkSocketRateLimit(socket.id)) {
                return safeCallback(callback, { success: false, error: 'Rate limit exceeded' });
            }
            if (!isValidRoomCode(roomCode)) {
                return safeCallback(callback, { success: false, error: 'Invalid room code' });
            }
            if (!peerId || typeof peerId !== 'string') {
                return safeCallback(callback, { success: false, error: 'Invalid peer ID' });
            }
            if (!isValidPeerName(peerName)) {
                return safeCallback(callback, { success: false, error: 'Invalid peer name' });
            }

            const currentPeer = peers.get(socket.id);
            if (!currentPeer || currentPeer.roomCode !== roomCode || currentPeer.id !== peerId) {
                return safeCallback(callback, { success: false, error: 'Peer not registered in this room' });
            }

            const roomPeers = rooms.get(roomCode);
            if (!roomPeers) {
                return safeCallback(callback, { success: false, error: 'Room not found' });
            }

            const updatedName = peerName.trim().substring(0, 50);
            currentPeer.name = updatedName;
            currentPeer.lastSeen = Date.now();

            const peerIndex = roomPeers.findIndex(peer => peer.id === peerId);
            if (peerIndex === -1) {
                return safeCallback(callback, { success: false, error: 'Peer not found in room' });
            }

            roomPeers[peerIndex] = {
                ...roomPeers[peerIndex], name: updatedName, lastSeen: currentPeer.lastSeen
            };

            const updatedPeer = roomPeers[peerIndex];
            io.to(roomCode).emit('peer-updated', updatedPeer);

            safeCallback(callback, { success: true, peer: updatedPeer });
            log('info', 'Peer name updated', { peerId, peerName: updatedName, roomCode });
        } catch (error) {
            log('error', 'Error in update-peer-name', { error: error.message });
            safeCallback(callback, { success: false, error: error.message });
        }
    });

    socket.on('heartbeat', ({ roomCode }) => {
        const peerInfo = peers.get(socket.id);
        if (peerInfo) {
            peerInfo.lastSeen = Date.now();
            peerInfo.isActive = true;

            const roomPeers = rooms.get(roomCode);
            if (roomPeers) {
                const index = roomPeers.findIndex(peer => peer.socketId === socket.id);
                if (index > -1) {
                    roomPeers[index].lastSeen = Date.now();
                }
            }
        }
    });

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
                    cancelTransfer(transfer, 'sender-offline');
                } else {
                    for (const [receiverPeerId, receiver] of transfer.receivers) {
                        if (receiver.socketId !== socket.id) {
                            continue;
                        }
                        removeReceiverFromTransfer(transfer, receiverPeerId, 'receiver-offline');
                        break;
                    }
                }
            }

            io.to(roomCode).emit('peer-left', { peerId: id });
            peers.delete(socket.id);

            log('info', 'Peer disconnected', { name, roomCode });
        }
    });
});

app.post('/api/transfers/:transferId/chunks/:chunkIndex', (req, res) => {
    try {
        const { transferId } = req.params;
        const chunkIndex = parsePositiveInteger(req.params.chunkIndex);
        const senderPeerId = req.header('x-peer-id');

        if (chunkIndex === null) {
            return res.status(400).json({ error: 'Invalid chunk index' });
        }

        const transfer = getTransferOrThrow(transferId);
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
            data: req.body, uploadedAt: Date.now(), size: req.body.length, hash: req.header('x-chunk-hash') || null
        });
        transfer.chunkStates.uploaded.add(chunkIndex);
        transfer.state = transfer.receivers.size > 0 ? 'transferring' : 'ready';
        updateTransferTimestamp(transfer);

        const file = files.get(transfer.fileId);
        if (file) {
            file.status = transfer.state;
        }

        io.to(transfer.roomCode).emit('transfer-updated', {
            transferId, fileId: transfer.fileId, status: mapTransferStatus(transfer), chunkIndex
        });

        res.status(202).json({
            success: true, transferId, chunkIndex, state: transfer.state
        });
    } catch (error) {
        log('error', 'Error in chunk upload', { error: error.message });
        res.status(error.message === 'Transfer not found' ? 404 : 400).json({ error: error.message });
    }
});

app.get('/api/transfers/:transferId/chunks/:chunkIndex', (req, res) => {
    try {
        const { transferId } = req.params;
        const chunkIndex = parsePositiveInteger(req.params.chunkIndex);
        const receiverPeerId = req.header('x-peer-id');

        if (chunkIndex === null) {
            return res.status(400).json({ error: 'Invalid chunk index' });
        }

        const transfer = getTransferOrThrow(transferId);
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
                cancelTransfer(transfer, 'expired');
                transfers.delete(transferId);
                cleanedTransfers++;
            }
        }

        if (cleanedFiles > 0 || cleanedTransfers > 0) {
            log('info', 'Cleanup: Removed expired resources', {
                files: cleanedFiles, transfers: cleanedTransfers
            });
        }
    } catch (error) {
        log('error', 'Cleanup error (resources)', { error: error.message });
    }
}, CLEANUP_INTERVAL);

setInterval(() => {
    try {
        const now = Date.now();

        for (const [socketId, peerInfo] of peers) {
            if (now - peerInfo.lastSeen > INACTIVE_TIMEOUT) {
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
                        cancelTransfer(transfer, 'sender-timeout');
                    } else {
                        for (const [receiverPeerId, receiver] of transfer.receivers) {
                            if (receiver.socketId !== socketId) {
                                continue;
                            }
                            removeReceiverFromTransfer(transfer, receiverPeerId, 'receiver-timeout', false);
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
}, CLEANUP_INTERVAL);

app.get('/health', (req, res) => {
    const memoryUsage = process.memoryUsage();
    res.json({
        status: 'ok',
        timestamp: new Date().toISOString(),
        environment: NODE_ENV,
        ip: LOCAL_IP,
        port: PORT,
        publicBaseUrl: getPublicBaseUrl(),
        uptime: Math.floor(process.uptime()),
        memory: {
            heapUsed: `${Math.round(memoryUsage.heapUsed / 1024 / 1024)}MB`,
            heapTotal: `${Math.round(memoryUsage.heapTotal / 1024 / 1024)}MB`,
            rss: `${Math.round(memoryUsage.rss / 1024 / 1024)}MB`
        },
        stats: {
            peers: peers.size, files: files.size, rooms: rooms.size, transfers: transfers.size
        }
    });
});

app.get('/api/server-info', (req, res) => {
    res.json({
        ip: LOCAL_IP, port: PORT, publicBaseUrl: getPublicBaseUrl(), environment: NODE_ENV, limits: {
            maxFileSize: MAX_FILE_SIZE,
            defaultChunkSize: DEFAULT_CHUNK_SIZE,
            maxChunkSize: MAX_CHUNK_SIZE,
            maxInflightChunks: MAX_INFLIGHT_CHUNKS,
            maxTransferBufferBytes: MAX_TRANSFER_BUFFER_BYTES
        }, stats: {
            totalPeers: peers.size, totalFiles: files.size, totalRooms: rooms.size, totalTransfers: transfers.size
        }
    });
});

app.get('/api/rooms/:roomCode', (req, res) => {
    const { roomCode } = req.params;

    if (!isValidRoomCode(roomCode)) {
        return res.status(400).json({ error: 'Invalid room code format' });
    }

    const roomPeers = rooms.get(roomCode);
    if (!roomPeers) {
        return res.status(404).json({ error: 'Room not found' });
    }

    const texts = getRoomTexts(roomCode)
        .slice()
        .sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt))
        .map(mapTextToResponse);

    return res.json({
        roomCode, peers: roomPeers, files: getRoomFiles(roomCode), texts, timestamp: new Date().toISOString()
    });
});

app.get('/api/transfers/:transferId', (req, res) => {
    try {
        const transfer = getTransferOrThrow(req.params.transferId);
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

app.use((req, res) => {
    res.status(404).json({
        error: 'Not found', path: req.path
    });
});

app.use((err, req, res, next) => {
    console.error('Error:', err);
    res.status(500).json({
        error: 'Internal server error', message: NODE_ENV === 'development' ? err.message : undefined
    });
});

server.listen(PORT, '0.0.0.0', () => {
    console.log(`
╔════════════════════════════════════════════════════════╗
║                  DropFile Server                       ║
╠════════════════════════════════════════════════════════╣
║ Status:   ✓ Running                                    ║
║ IP:       ${LOCAL_IP.padEnd(45)}                       ║
║ Port:     ${PORT.toString().padEnd(45)}                ║
║ Env:      ${NODE_ENV.padEnd(45)}                       ║
║ URL:      http://${LOCAL_IP}:${PORT}                   ║
╠════════════════════════════════════════════════════════╣
║ Health:   http://${LOCAL_IP}:${PORT}/health            ║
║ Info:     http://${LOCAL_IP}:${PORT}/api/server-info   ║
╚════════════════════════════════════════════════════════╝
`);
});

function gracefulShutdown(signal) {
    log('info', `Shutting down server (${signal})...`);
    io.close();
    server.close(() => {
        log('info', 'Server stopped gracefully');
        process.exit(0);
    });

    setTimeout(() => {
        log('warn', 'Forcing shutdown after timeout');
        process.exit(1);
    }, 10000);
}

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
