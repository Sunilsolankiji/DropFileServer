# DropFile Backend Server

Backend server for cross-device file sharing over local network/WiFi.

## Features

- ✅ Real-time file sharing across multiple devices on same network
- ✅ Automatic file expiration (15 minutes)
- ✅ Socket.IO for reliable WebSocket communication
- ✅ REST API for server health checks
- ✅ Automatic cleanup of expired files and inactive peers
- ✅ Multi-room support
- ✅ Base64 file encoding for easy transfer

## Installation

```bash
npm install
```

## Setup

1. Create `.env` file (optional, defaults are provided):

```env
PORT=3001
NODE_ENV=development
```

2. Install dependencies:

```bash
npm install
```

## Running

### Development (with auto-reload):
```bash
npm run dev
```

### Production:
```bash
npm start
```

## Server API

### Socket.IO Events

#### Client → Server

- **join-room**: Join a sharing room
  ```javascript
  socket.emit('join-room', {
    roomCode: 'ABC123',
    peerName: 'My Device',
    peerId: 'peer_xxx'
  }, callback)
  ```

- **add-file**: Add a file to share
  ```javascript
  socket.emit('add-file', {
    roomCode: 'ABC123',
    file: {
      id: 'file_xxx',
      name: 'document.pdf',
      size: 1024000,
      type: 'application/pdf',
      data: 'base64_encoded_file_data'
    },
    peerId: 'peer_xxx',
    peerName: 'My Device'
  }, callback)
  ```

- **download-file**: Download a file
  ```javascript
  socket.emit('download-file', {
    fileId: 'file_xxx'
  }, callback)
  ```

- **remove-file**: Remove a shared file
  ```javascript
  socket.emit('remove-file', {
    fileId: 'file_xxx',
    roomCode: 'ABC123'
  }, callback)
  ```

- **heartbeat**: Send heartbeat (to keep connection alive)
  ```javascript
  socket.emit('heartbeat', {
    peerId: 'peer_xxx',
    roomCode: 'ABC123'
  })
  ```

#### Server → Client

- **peer-joined**: New peer joined the room
  ```javascript
  socket.on('peer-joined', (peerInfo) => {
    // peerInfo: { id, name, joinedAt, ip, ... }
  })
  ```

- **file-added**: New file available in room
  ```javascript
  socket.on('file-added', (fileInfo) => {
    // fileInfo: { id, name, size, type, peerId, peerName, expiresAt }
  })
  ```

- **file-removed**: File removed from sharing
  ```javascript
  socket.on('file-removed', ({ fileId }) => {
    // Handle file removal
  })
  ```

- **peer-left**: Peer disconnected from room
  ```javascript
  socket.on('peer-left', ({ peerId }) => {
    // Handle peer leaving
  })
  ```

### REST API

- **GET /health**
  - Health check endpoint
  - Returns: `{ status: 'ok', timestamp, environment, ip, port }`

- **GET /api/server-info**
  - Server information and stats
  - Returns: `{ ip, port, environment, stats: { totalPeers, totalFiles, totalRooms } }`

- **GET /api/rooms/:roomCode**
  - Get room information
  - Returns: `{ roomCode, peers, files, timestamp }`

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Device A   │     │  Device B   │     │  Device C   │
│  (Browser)  │     │  (Browser)  │     │  (Browser)  │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                    │                    │
       └────────────────────┼────────────────────┘
                            │
                      ┌─────▼─────┐
                      │   Server   │
                      │ (Node.js)  │
                      └────────────┘
```

## Data Structures

### Peer
```javascript
{
  id: 'peer_xxx',
  socketId: 'socket_xxx',
  name: 'Device Name',
  joinedAt: timestamp,
  lastSeen: timestamp,
  ip: '192.168.x.x',
  isActive: boolean,
  roomCode: 'ABC123'
}
```

### File
```javascript
{
  id: 'file_xxx',
  name: 'filename.ext',
  size: bytes,
  type: 'mime/type',
  peerId: 'peer_xxx',
  peerName: 'Device Name',
  roomCode: 'ABC123',
  expiresAt: timestamp,
  data: 'base64_encoded_data',
  uploadedAt: timestamp
}
```

## Performance Notes

- Max file size: 100MB (configurable in server.js)
- File TTL: 15 minutes
- Peer heartbeat: 5 seconds (auto-removes after 30 seconds inactivity)
- Cleanup interval: 30 seconds

## Debugging

Enable verbose logging by setting environment variable:
```bash
NODE_ENV=development npm run dev
```

## Port Forwarding (for internet access)

To access the server outside your local network:
1. Forward port 3001 on your router
2. Use your public IP address
3. Note: This exposes files to the internet - use with caution

## License

MIT

