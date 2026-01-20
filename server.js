import express from 'express';
import http from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import { v4 as uuidv4 } from 'uuid';
import os from 'os';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 3001;
const NODE_ENV = process.env.NODE_ENV || 'development';

// Configure Socket.IO with CORS
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  },
  transports: ['websocket'], // Use websocket only to prevent duplicate connections from transport upgrade
  maxHttpBufferSize: 1e8 // 100MB for file transfers
});

// Middleware
app.use(cors());
app.use(express.json({ limit: '100mb' }));
app.use(express.urlencoded({ limit: '100mb', extended: true }));

// Data structures
const peers = new Map(); // socketId -> peer info
const files = new Map(); // fileId -> file info
const rooms = new Map(); // roomCode -> array of peers

// Constants
const FILE_TTL_MS = 15 * 60 * 1000; // 15 minutes
const PEER_HEARTBEAT_INTERVAL = 5000; // 5 seconds
const CLEANUP_INTERVAL = 30000; // 30 seconds

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
  console.log(`[${new Date().toISOString()}] Peer connected: ${socket.id}`);

  /**
   * Peer joins a room with unique identifier
   */
  socket.on('join-room', ({ roomCode, peerName, peerId }, callback) => {
    try {
      // Check if this socket is already registered (prevent duplicate join-room calls)
      if (peers.has(socket.id)) {
        const existingPeer = peers.get(socket.id);
        console.log(`[WARN] Socket ${socket.id} already joined as ${existingPeer.name}, ignoring duplicate join-room`);

        const roomPeers = rooms.get(roomCode) || [];
        const otherPeers = roomPeers.filter(p => p.socketId !== socket.id);
        const roomFiles = Array.from(files.values()).filter(f => f.roomCode === roomCode);

        callback({
          success: true,
          peers: otherPeers,
          files: roomFiles.map(f => ({
            id: f.id,
            name: f.name,
            size: f.size,
            type: f.type,
            peerId: f.peerId,
            peerName: f.peerName,
            expiresAt: f.expiresAt
          })),
          serverInfo: {
            ip: LOCAL_IP,
            port: PORT
          }
        });
        return;
      }

      socket.join(roomCode);

      // Initialize room if it doesn't exist
      if (!rooms.has(roomCode)) {
        rooms.set(roomCode, []);
      }

      const peerInfo = {
        id: peerId,
        socketId: socket.id,
        name: peerName || `Device ${peerId.slice(0, 6)}`,
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

      callback({
        success: true,
        peers: otherPeers,
        files: roomFiles.map(f => ({
          id: f.id,
          name: f.name,
          size: f.size,
          type: f.type,
          peerId: f.peerId,
          peerName: f.peerName,
          expiresAt: f.expiresAt
        })),
        serverInfo: {
          ip: LOCAL_IP,
          port: PORT
        }
      });

      console.log(`${peerInfo.name} joined room ${roomCode} (isNew: ${isNewPeer}, roomPeers: ${roomPeers.length}, totalPeers: ${peers.size})`);
    } catch (error) {
      console.error('Error in join-room:', error);
      callback({
        success: false,
        error: error.message
      });
    }
  });

  /**
   * Peer adds a file to share
   */
  socket.on('add-file', ({ roomCode, file, peerId, peerName }, callback) => {
    try {
      const fileId = file.id || uuidv4();
      const expiresAt = Date.now() + FILE_TTL_MS;

      const fileData = {
        id: fileId,
        name: file.name,
        size: file.size,
        type: file.type,
        peerId,
        peerName,
        roomCode,
        expiresAt,
        data: file.data, // Base64 encoded
        uploadedAt: Date.now()
      };

      files.set(fileId, fileData);

      // Broadcast file metadata to all peers in room
      io.to(roomCode).emit('file-added', {
        id: fileId,
        name: file.name,
        size: file.size,
        type: file.type,
        peerId,
        peerName,
        expiresAt,
        uploadedAt: fileData.uploadedAt
      });

      callback({
        success: true,
        fileId,
        expiresAt
      });

      console.log(`File added: ${file.name} (${fileId}) by ${peerName}`);
    } catch (error) {
      console.error('Error in add-file:', error);
      callback({
        success: false,
        error: error.message
      });
    }
  });

  /**
   * Peer requests file download
   */
  socket.on('download-file', ({ fileId }, callback) => {
    try {
      const file = files.get(fileId);

      if (!file) {
        callback({
          success: false,
          error: 'File not found or expired'
        });
        return;
      }

      // Check if file is expired
      if (file.expiresAt < Date.now()) {
        files.delete(fileId);
        callback({
          success: false,
          error: 'File has expired'
        });
        return;
      }

      // Send file data
      callback({
        success: true,
        file: {
          id: file.id,
          name: file.name,
          size: file.size,
          type: file.type,
          data: file.data
        }
      });

      console.log(`File downloaded: ${file.name} (${fileId})`);
    } catch (error) {
      console.error('Error in download-file:', error);
      callback({
        success: false,
        error: error.message
      });
    }
  });

  /**
   * Peer removes a file from sharing
   */
  socket.on('remove-file', ({ fileId, roomCode }, callback) => {
    try {
      const file = files.get(fileId);

      if (file && file.roomCode === roomCode) {
        files.delete(fileId);
        io.to(roomCode).emit('file-removed', { fileId });

        callback({ success: true });
        console.log(`File removed: ${fileId}`);
      } else {
        callback({
          success: false,
          error: 'File not found'
        });
      }
    } catch (error) {
      console.error('Error in remove-file:', error);
      callback({
        success: false,
        error: error.message
      });
    }
  });

  /**
   * Peer sends heartbeat to stay active
   */
  socket.on('heartbeat', ({ peerId, roomCode }) => {
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
          console.log(`Room ${roomCode} cleaned up (empty)`);
        }
      }

      // Notify others about peer leaving
      io.to(roomCode).emit('peer-left', { peerId: id });
      peers.delete(socket.id);

      console.log(`${name} disconnected from room ${roomCode}`);
    }
  });
});

/**
 * Cleanup expired files every 30 seconds
 */
setInterval(() => {
  const now = Date.now();
  let cleaned = 0;

  for (const [fileId, file] of files) {
    if (file.expiresAt < now) {
      files.delete(fileId);
      cleaned++;
    }
  }

  if (cleaned > 0) {
    console.log(`[Cleanup] Removed ${cleaned} expired files`);
  }
}, CLEANUP_INTERVAL);

/**
 * Cleanup inactive peers every 30 seconds
 */
setInterval(() => {
  const now = Date.now();
  const INACTIVE_TIMEOUT = 30000; // 30 seconds

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
        }
      }

      io.to(peerInfo.roomCode).emit('peer-left', { peerId: peerInfo.id });
      peers.delete(socketId);
      console.log(`[Cleanup] Removed inactive peer: ${peerInfo.name}`);
    }
  }
}, CLEANUP_INTERVAL);

/**
 * REST API endpoints
 */

// Health check
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    environment: NODE_ENV,
    ip: LOCAL_IP,
    port: PORT
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
  const roomPeers = rooms.get(roomCode);

  if (!roomPeers) {
    return res.status(404).json({
      error: 'Room not found'
    });
  }

  const roomFiles = Array.from(files.values())
    .filter(f => f.roomCode === roomCode)
    .map(f => ({
      id: f.id,
      name: f.name,
      size: f.size,
      type: f.type,
      peerId: f.peerId,
      peerName: f.peerName,
      expiresAt: f.expiresAt
    }));

  res.json({
    roomCode,
    peers: roomPeers,
    files: roomFiles,
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
process.on('SIGINT', () => {
  console.log('\nShutting down server...');
  server.close(() => {
    console.log('Server stopped');
    process.exit(0);
  });
});

process.on('SIGTERM', () => {
  console.log('\nShutting down server (SIGTERM)...');
  server.close(() => {
    console.log('Server stopped');
    process.exit(0);
  });
});

