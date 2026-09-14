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

// Configuration from environment
const PORT = process.env.PORT || 3001;
const NODE_ENV = process.env.NODE_ENV || 'development';
const ALLOWED_ORIGINS = process.env.ALLOWED_ORIGINS?.split(',') || ['http://localhost:3000', 'http://localhost:9002', 'https://sunilsolankiji.github.io/DropFile'];
const MAX_FILE_SIZE = parseInt(process.env.MAX_FILE_SIZE) || 100 * 1024 * 1024; // 100MB
const FILE_TTL_MS = parseInt(process.env.FILE_TTL_MS) || 15 * 60 * 1000; // 15 minutes
const CLEANUP_INTERVAL = parseInt(process.env.CLEANUP_INTERVAL) || 30000; // 30 seconds
const MAX_FILES_PER_ROOM = parseInt(process.env.MAX_FILES_PER_ROOM) || 50;
const INACTIVE_TIMEOUT = parseInt(process.env.INACTIVE_TIMEOUT) || 30000; // 30 seconds

// CORS configuration
const corsOptions = {
  origin: NODE_ENV === 'development' ? '*' : ALLOWED_ORIGINS,
  methods: ['GET', 'POST']
};

// Configure Socket.IO with CORS
const io = new Server(server, {
  cors: corsOptions,
  transports: ['websocket'], // Use websocket only to prevent duplicate connections from transport upgrade
  maxHttpBufferSize: MAX_FILE_SIZE
});

// Security middleware
app.use(helmet({
  crossOriginResourcePolicy: { policy: 'cross-origin' }
}));

// Rate limiting for API endpoints
const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // Limit each IP to 100 requests per windowMs
  message: { error: 'Too many requests, please try again later.' }
});
app.use('/api/', apiLimiter);

// Middleware
app.use(cors(corsOptions));
app.use(express.json({ limit: '100mb' }));
app.use(express.urlencoded({ limit: '100mb', extended: true }));

// Data structures
const peers = new Map(); // socketId -> peer info
const files = new Map(); // fileId -> file info
const rooms = new Map(); // roomCode -> array of peers
const roomTexts = new Map(); // roomCode -> array of text message info

// Socket rate limiting map
const socketRateLimits = new Map(); // socketId -> { count, resetTime }
const SOCKET_RATE_LIMIT = 30; // Max events per second
const SOCKET_RATE_WINDOW = 1000; // 1 second

// ============================================
// HELPER FUNCTIONS
// ============================================

/**
 * Log with timestamp and level
 */
function log(level, message, data = {}) {
  const timestamp = new Date().toISOString();
  const dataStr = Object.keys(data).length ? ` ${JSON.stringify(data)}` : '';
  console.log(`[${timestamp}] [${level.toUpperCase()}] ${message}${dataStr}`);
}

/**
 * Validate room code format
 */
function isValidRoomCode(roomCode) {
  if (!roomCode || typeof roomCode !== 'string') return false;
  // Allow alphanumeric, hyphens, underscores, 3-50 chars
  return /^[a-zA-Z0-9_-]{3,50}$/.test(roomCode);
}

/**
 * Validate peer name
 */
function isValidPeerName(name) {
  if (!name || typeof name !== 'string') return false;
  // Allow reasonable peer names, 1-50 chars, no script injection
  return name.length >= 1 && name.length <= 50 && !/<[^>]*>/.test(name);
}

/**
 * Validate file data
 */
function isValidFile(file) {
  if (!file || typeof file !== 'object') return false;
  if (!file.name || typeof file.name !== 'string' || file.name.length > 255) return false;
  if (typeof file.size !== 'number' || file.size <= 0 || file.size > MAX_FILE_SIZE) return false;
  if (!file.data || typeof file.data !== 'string') return false;
  return true;
}

/**
 * Validate text message payload
 */
function isValidTextMessage(message) {
  if (!message || typeof message !== 'object') return false;
  if (message.peerName && !isValidPeerName(message.peerName)) return false;

  const messageText = typeof message.text === 'string'
    ? message.text
    : typeof message.message === 'string'
      ? message.message
      : null;

  if (messageText === null) return false;

  const trimmedText = messageText.trim();
  return trimmedText.length >= 1 && trimmedText.length <= 2000;
}

/**
 * Map text message to response
 */
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

/**
 * Check socket rate limit
 */
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

/**
 * Safe callback wrapper
 */
function safeCallback(callback, response) {
  if (typeof callback === 'function') {
    try {
      callback(response);
    } catch (err) {
      log('error', 'Callback error', { error: err.message });
    }
  }
}

/**
 * Map file to response (excludes file data)
 */
function mapFileToResponse(file) {
  return {
    id: file.id,
    name: file.name,
    size: file.size,
    type: file.type,
    peerId: file.peerId,
    peerName: file.peerName,
    expiresAt: file.expiresAt,
    uploadedAt: file.uploadedAt
  };
}

/**
 * Get local IP address
 */
function getLocalIP() {
  const interfaces = os.networkInterfaces();
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      // Skip internal and non-IPv4 addresses
      if (iface.family === 'IPv4' && !iface.internal) {
        return iface.address;
      }
    }
  }
  return 'localhost';
}

const LOCAL_IP = getLocalIP();

/**
 * Initialize Socket.IO connection handlers
 */
io.on('connection', (socket) => {
  log('info', 'Peer connected', { socketId: socket.id });

  /**
   * Peer joins a room with unique identifier
   */
  socket.on('join-room', ({ roomCode, peerName, peerId }, callback) => {
    try {
      // Rate limit check
      if (!checkSocketRateLimit(socket.id)) {
        return safeCallback(callback, { success: false, error: 'Rate limit exceeded' });
      }

      // Input validation
      if (!isValidRoomCode(roomCode)) {
        return safeCallback(callback, { success: false, error: 'Invalid room code format' });
      }
      if (peerName && !isValidPeerName(peerName)) {
        return safeCallback(callback, { success: false, error: 'Invalid peer name' });
      }
      if (!peerId || typeof peerId !== 'string') {
        return safeCallback(callback, { success: false, error: 'Invalid peer ID' });
      }

      // Check if this socket is already registered (prevent duplicate join-room calls)
      if (peers.has(socket.id)) {
        const existingPeer = peers.get(socket.id);
        log('warn', 'Duplicate join-room attempt', { socketId: socket.id, existingName: existingPeer.name });

        const roomPeers = rooms.get(roomCode) || [];
        const otherPeers = roomPeers.filter(p => p.socketId !== socket.id);
        const roomFiles = Array.from(files.values()).filter(f => f.roomCode === roomCode);
        const texts = getRoomTexts(roomCode)
          .slice()
          .sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt))
          .map(mapTextToResponse);

        return safeCallback(callback, {
          success: true,
          peers: otherPeers,
          files: roomFiles.map(mapFileToResponse),
          texts,
          serverInfo: { ip: LOCAL_IP, port: PORT }
        });
      }

      socket.join(roomCode);

      // Initialize room if it doesn't exist
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

      // Check if peer already exists in room (by peerId) to prevent duplicates
      const existingPeerIndex = roomPeers.findIndex(p => p.id === peerId);
      const isNewPeer = existingPeerIndex === -1;

      if (existingPeerIndex > -1) {
        // Remove old socket entry from peers Map
        const oldSocketId = roomPeers[existingPeerIndex].socketId;
        if (oldSocketId !== socket.id) {
          peers.delete(oldSocketId);
        }
        // Update existing peer's socket info
        roomPeers[existingPeerIndex] = peerInfo;
      } else {
        roomPeers.push(peerInfo);
      }

      // Add new socket to peers Map
      peers.set(socket.id, { ...peerInfo, roomCode });

      // Notify others in the room about new peer (only if truly new)
      if (isNewPeer) {
        socket.to(roomCode).emit('peer-joined', peerInfo);
      }

      // Send existing peers and files to the new joiner
      const otherPeers = roomPeers.filter(p => p.socketId !== socket.id);
      const roomFiles = Array.from(files.values()).filter(f => f.roomCode === roomCode);
      const texts = getRoomTexts(roomCode)
        .slice()
        .sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt))
        .map(mapTextToResponse);

      safeCallback(callback, {
        success: true,
        peers: otherPeers,
        files: roomFiles.map(mapFileToResponse),
        texts,
        serverInfo: { ip: LOCAL_IP, port: PORT }
      });

      log('info', 'Peer joined room', {
        name: peerInfo.name,
        roomCode,
        isNew: isNewPeer,
        roomPeers: roomPeers.length
      });
    } catch (error) {
      log('error', 'Error in join-room', { error: error.message });
      safeCallback(callback, { success: false, error: error.message });
    }
  });

  /**
   * Peer adds a file to share
   */
  socket.on('add-file', ({ roomCode, file, peerId, peerName }, callback) => {
    try {
      // Rate limit check
      if (!checkSocketRateLimit(socket.id)) {
        return safeCallback(callback, { success: false, error: 'Rate limit exceeded' });
      }

      // Input validation
      if (!isValidRoomCode(roomCode)) {
        return safeCallback(callback, { success: false, error: 'Invalid room code' });
      }
      if (!isValidFile(file)) {
        return safeCallback(callback, { success: false, error: 'Invalid file data or file too large' });
      }

      // Check room file limit
      const roomFiles = Array.from(files.values()).filter(f => f.roomCode === roomCode);
      if (roomFiles.length >= MAX_FILES_PER_ROOM) {
        return safeCallback(callback, { success: false, error: `Room file limit (${MAX_FILES_PER_ROOM}) reached` });
      }

      const fileId = file.id || uuidv4();
      const expiresAt = Date.now() + FILE_TTL_MS;

      const fileData = {
        id: fileId,
        name: file.name.substring(0, 255), // Sanitize filename length
        size: file.size,
        type: file.type || 'application/octet-stream',
        peerId,
        peerName: peerName?.substring(0, 50) || 'Unknown',
        roomCode,
        expiresAt,
        data: file.data, // Base64 encoded
        uploadedAt: Date.now(),
        ownerSocketId: socket.id // Track ownership for authorization
      };

      files.set(fileId, fileData);

      // Broadcast file metadata to all peers in room
      io.to(roomCode).emit('file-added', mapFileToResponse(fileData));

      safeCallback(callback, { success: true, fileId, expiresAt });

      log('info', 'File added', { fileName: file.name, fileId, peerName, roomCode });
    } catch (error) {
      log('error', 'Error in add-file', { error: error.message });
      safeCallback(callback, { success: false, error: error.message });
    }
  });

  /**
   * Peer adds a text message to a room
   */
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
        createdAt: text.createdAt && !Number.isNaN(Date.parse(text.createdAt))
          ? text.createdAt
          : new Date().toISOString(),
        ownerSocketId: socket.id
      };

      const messages = getRoomTexts(roomCode);
      messages.push(message);
      roomTexts.set(roomCode, messages);

      socket.to(roomCode).emit('text-added', mapTextToResponse(message));

      safeCallback(callback, {
        success: true,
        message: mapTextToResponse(message)
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
        ...roomPeers[peerIndex],
        name: updatedName,
        lastSeen: currentPeer.lastSeen
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

  /**
   * Peer requests file download
   */
  socket.on('download-file', ({ fileId }, callback) => {
    try {
      // Rate limit check
      if (!checkSocketRateLimit(socket.id)) {
        return safeCallback(callback, { success: false, error: 'Rate limit exceeded' });
      }

      const file = files.get(fileId);

      if (!file) {
        return safeCallback(callback, { success: false, error: 'File not found or expired' });
      }

      // Check if file is expired
      if (file.expiresAt < Date.now()) {
        files.delete(fileId);
        return safeCallback(callback, { success: false, error: 'File has expired' });
      }

      // Send file data
      safeCallback(callback, {
        success: true,
        file: {
          id: file.id,
          name: file.name,
          size: file.size,
          type: file.type,
          data: file.data
        }
      });

      log('info', 'File downloaded', { fileName: file.name, fileId });
    } catch (error) {
      log('error', 'Error in download-file', { error: error.message });
      safeCallback(callback, { success: false, error: error.message });
    }
  });

  /**
   * Peer removes a file from sharing
   */
  socket.on('remove-file', ({ fileId, roomCode }, callback) => {
    try {
      // Rate limit check
      if (!checkSocketRateLimit(socket.id)) {
        return safeCallback(callback, { success: false, error: 'Rate limit exceeded' });
      }

      const file = files.get(fileId);

      if (!file) {
        return safeCallback(callback, { success: false, error: 'File not found' });
      }

      if (file.roomCode !== roomCode) {
        return safeCallback(callback, { success: false, error: 'File not in this room' });
      }

      // Authorization: Only file owner can delete
      const peer = peers.get(socket.id);
      if (file.peerId !== peer?.id && file.ownerSocketId !== socket.id) {
        return safeCallback(callback, { success: false, error: 'Unauthorized: Only file owner can remove' });
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

  /**
   * Peer sends heartbeat to stay active
   */
  socket.on('heartbeat', ({ roomCode }) => {
    const peerInfo = peers.get(socket.id);
    if (peerInfo) {
      peerInfo.lastSeen = Date.now();
      peerInfo.isActive = true;

      // Update peer in room
      const roomPeers = rooms.get(roomCode);
      if (roomPeers) {
        const index = roomPeers.findIndex(p => p.socketId === socket.id);
        if (index > -1) {
          roomPeers[index].lastSeen = Date.now();
        }
      }
    }
  });

  /**
   * Handle peer disconnect
   */
  socket.on('disconnect', () => {
    const peerInfo = peers.get(socket.id);

    // Clean up rate limit tracking
    socketRateLimits.delete(socket.id);

    if (peerInfo) {
      const { roomCode, name, id } = peerInfo;
      const roomPeers = rooms.get(roomCode);

      if (roomPeers) {
        const index = roomPeers.findIndex(p => p.socketId === socket.id);
        if (index > -1) {
          roomPeers.splice(index, 1);
        }

        // Clean up room if empty
        if (roomPeers.length === 0) {
          rooms.delete(roomCode);
          roomTexts.delete(roomCode);
          log('info', 'Room cleaned up (empty)', { roomCode });
        }
      }

      // Notify others about peer leaving
      io.to(roomCode).emit('peer-left', { peerId: id });
      peers.delete(socket.id);

      log('info', 'Peer disconnected', { name, roomCode });
    }
  });
});

/**
 * Cleanup expired files every 30 seconds
 */
setInterval(() => {
  try {
    const now = Date.now();
    let cleaned = 0;

    for (const [fileId, file] of files) {
      if (file.expiresAt < now) {
        files.delete(fileId);
        cleaned++;
      }
    }

    if (cleaned > 0) {
      log('info', 'Cleanup: Removed expired files', { count: cleaned });
    }
  } catch (error) {
    log('error', 'Cleanup error (files)', { error: error.message });
  }
}, CLEANUP_INTERVAL);

/**
 * Cleanup inactive peers every 30 seconds
 */
setInterval(() => {
  try {
    const now = Date.now();

    for (const [socketId, peerInfo] of peers) {
      if (now - peerInfo.lastSeen > INACTIVE_TIMEOUT) {
        // Remove from roomPeers array
        const roomPeers = rooms.get(peerInfo.roomCode);
        if (roomPeers) {
          const index = roomPeers.findIndex(p => p.socketId === socketId);
          if (index > -1) {
            roomPeers.splice(index, 1);
          }
          // Clean up empty rooms
          if (roomPeers.length === 0) {
            rooms.delete(peerInfo.roomCode);
            roomTexts.delete(peerInfo.roomCode);
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

/**
 * REST API endpoints
 */

// Health check
app.get('/health', (req, res) => {
  const memoryUsage = process.memoryUsage();
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    environment: NODE_ENV,
    ip: LOCAL_IP,
    port: PORT,
    uptime: Math.floor(process.uptime()),
    memory: {
      heapUsed: `${Math.round(memoryUsage.heapUsed / 1024 / 1024)}MB`,
      heapTotal: `${Math.round(memoryUsage.heapTotal / 1024 / 1024)}MB`,
      rss: `${Math.round(memoryUsage.rss / 1024 / 1024)}MB`
    },
    stats: {
      peers: peers.size,
      files: files.size,
      rooms: rooms.size
    }
  });
});

// Get server info
app.get('/api/server-info', (req, res) => {
  res.json({
    ip: LOCAL_IP,
    port: PORT,
    environment: NODE_ENV,
    stats: {
      totalPeers: peers.size,
      totalFiles: files.size,
      totalRooms: rooms.size
    }
  });
});

// Get room info
app.get('/api/rooms/:roomCode', (req, res) => {
  const { roomCode } = req.params;

  // Validate room code
  if (!isValidRoomCode(roomCode)) {
    return res.status(400).json({ error: 'Invalid room code format' });
  }

  const roomPeers = rooms.get(roomCode);

  if (!roomPeers) {
    return res.status(404).json({ error: 'Room not found' });
  }

  const roomFiles = Array.from(files.values())
    .filter(f => f.roomCode === roomCode)
    .map(mapFileToResponse);
  const texts = getRoomTexts(roomCode)
    .slice()
    .sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt))
    .map(mapTextToResponse);

  res.json({
    roomCode,
    peers: roomPeers,
    files: roomFiles,
    texts,
    timestamp: new Date().toISOString()
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    error: 'Not found',
    path: req.path
  });
});

// Error handler
app.use((err, req, res, next) => {
  console.error('Error:', err);
  res.status(500).json({
    error: 'Internal server error',
    message: NODE_ENV === 'development' ? err.message : undefined
  });
});

/**
 * Start server
 */
server.listen(PORT, '0.0.0.0', () => {
  console.log(`
╔════════════════════════════════════════════════════════╗
║                  DropFile Server                        ║
╠════════════════════════════════════════════════════════╣
║ Status:   ✓ Running                                     ║
║ IP:       ${LOCAL_IP.padEnd(45)} ║
║ Port:     ${PORT.toString().padEnd(45)} ║
║ Env:      ${NODE_ENV.padEnd(45)} ║
║ URL:      http://${LOCAL_IP}:${PORT}                    ║
╠════════════════════════════════════════════════════════╣
║ Health:   http://${LOCAL_IP}:${PORT}/health            ║
║ Info:     http://${LOCAL_IP}:${PORT}/api/server-info   ║
╚════════════════════════════════════════════════════════╝
`);
});

// Graceful shutdown
function gracefulShutdown(signal) {
  log('info', `Shutting down server (${signal})...`);

  // Close all socket connections
  io.close();

  server.close(() => {
    log('info', 'Server stopped gracefully');
    process.exit(0);
  });

  // Force exit after 10 seconds if graceful shutdown fails
  setTimeout(() => {
    log('warn', 'Forcing shutdown after timeout');
    process.exit(1);
  }, 10000);
}

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
