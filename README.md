# DropFile Backend Server

Backend server for cross-device file sharing over local network/WiFi.

## Features

- ✅ Real-time room presence and text sharing over Socket.IO
- ✅ Large-file sharing with chunk relay instead of base64 whole-file storage
- ✅ Resumable online-only transfers with sender/receiver progress tracking
- ✅ HTTP chunk upload/download endpoints with bounded in-memory buffering
- ✅ Automatic cleanup of expired transfers, files, and inactive peers
- ✅ Multi-room support

## Installation

```bash
npm install
```

## Setup

Create `.env` if needed:

```env
PORT=3001
NODE_ENV=development
PUBLIC_BASE_URL=http://localhost:3001
ALLOWED_ORIGINS=http://localhost:3000,http://localhost:5173,https://sunilsolankiji.github.io
MAX_FILE_SIZE=1073741824
CHUNK_SIZE=1048576
MAX_CHUNK_SIZE=4194304
MAX_INFLIGHT_CHUNKS=8
MAX_TRANSFER_BUFFER_BYTES=67108864
FILE_TTL_MS=3600000
```

`PUBLIC_BASE_URL` must be the public backend origin when the frontend is hosted separately. For Render, set it to your service URL, for example `https://your-dropfile-server.onrender.com`. `ALLOWED_ORIGINS` must contain frontend origins only, without paths; use `https://sunilsolankiji.github.io`, not `https://sunilsolankiji.github.io/DropFile`.

## Running

### Development
```bash
npm run dev
```

### Production
```bash
npm start
```

## Project structure

`server.js` is a thin entry point. Application code is organized under `src/` by responsibility:

```text
src/
├── app/              # Express bootstrap, middleware, cleanup, shutdown wiring
├── config/           # Environment-backed configuration
├── infrastructure/   # Shared in-memory state
├── routes/           # HTTP API route modules
├── services/         # Peer, text, file, and transfer domain logic
├── sockets/          # Socket.IO registration and event handlers
└── utils/            # Logging, validation, callbacks, and network helpers
```

### Render free deployment

Use a **Web Service** with:

| Setting | Value |
|---|---|
| Build Command | `npm install` |
| Start Command | `npm start` |
| `NODE_ENV` | `production` |
| `PUBLIC_BASE_URL` | `https://<your-render-service>.onrender.com` |
| `ALLOWED_ORIGINS` | Your frontend origin, for example `https://sunilsolankiji.github.io` |

Render free instances sleep when idle. Because transfers are online-only and kept in memory, an active transfer is lost if the service restarts or sleeps; keep both devices connected while transferring and retry after the service wakes.

## Transfer architecture

Files are now represented as **share metadata + transfer session state**.

1. Sender joins a room.
2. Sender emits `add-file` with file metadata only.
3. Server creates a transfer session and broadcasts a file/share entry.
4. Receiver emits `start-transfer`.
5. Sender uploads numbered chunks to `POST /api/transfers/:transferId/chunks/:chunkIndex`.
6. Receiver fetches available chunks from `GET /api/transfers/:transferId/chunks/:chunkIndex`.
7. Receiver acknowledges chunks over Socket.IO with `ack-transfer-chunk`.
8. Server keeps only a bounded chunk window in memory and never stores the whole file payload.

This implementation is **online-only**. If the sender disconnects or the transfer expires, the transfer is cancelled.

## Socket.IO API

### Client → Server

- **join-room**
  ```javascript
  socket.emit('join-room', {
    roomCode: 'ABC123',
    peerName: 'My Device',
    peerId: 'peer_xxx'
  }, callback)
  ```

- **add-file**  
  Announces a share and creates a transfer session.
  ```javascript
  socket.emit('add-file', {
    roomCode: 'ABC123',
    peerId: 'peer_xxx',
    file: {
      id: 'file_xxx',
      name: 'video.mp4',
      size: 73400320,
      type: 'video/mp4',
      chunkSize: 1048576,
      totalChunks: 70,
      hash: 'optional-whole-file-hash'
    }
  }, callback)
  ```

- **start-transfer**
  ```javascript
  socket.emit('start-transfer', {
    roomCode: 'ABC123',
    fileId: 'file_xxx',
    peerId: 'receiver_peer'
  }, callback)
  ```

- **get-transfer-state**
  ```javascript
  socket.emit('get-transfer-state', {
    transferId: 'transfer_xxx'
  }, callback)
  ```

- **ack-transfer-chunk**
  ```javascript
  socket.emit('ack-transfer-chunk', {
    transferId: 'transfer_xxx',
    chunkIndex: 4,
    peerId: 'receiver_peer'
  }, callback)
  ```

- **cancel-transfer**
  ```javascript
  socket.emit('cancel-transfer', {
    transferId: 'transfer_xxx',
    roomCode: 'ABC123',
    peerId: 'peer_xxx',
    reason: 'user-cancelled'
  }, callback)
  ```

- **download-file**  
  Compatibility lookup that returns metadata and transfer info instead of file bytes.

- **remove-file**

- **add-text**

- **update-peer-name**

- **heartbeat**

### Server → Client

- **peer-joined**
- **peer-left**
- **peer-updated**
- **text-added**
- **file-added** — share metadata only, no `file.data`
- **file-removed**
- **transfer-updated**
  ```javascript
  socket.on('transfer-updated', ({ transferId, fileId, status, chunkIndex, reason }) => {
    // status: { state, uploadedChunks, acknowledgedChunks, senderConnected, receiverConnected, receiverPeerId }
  })
  ```
- **transfer-completed**

## HTTP API

- **GET /health**
- **GET /api/server-info**
- **GET /api/rooms/:roomCode**
- **GET /api/transfers/:transferId**
- **POST /api/transfers/:transferId/chunks/:chunkIndex**
  - Headers:
    - `x-peer-id`: sender peer id
    - `x-chunk-hash`: optional chunk hash
  - Body: raw binary chunk

- **GET /api/transfers/:transferId/chunks/:chunkIndex**
  - Headers:
    - `x-peer-id`: receiver peer id
  - Response: raw binary chunk when available

## Share object shape

```javascript
{
  id: 'file_xxx',
  name: 'video.mp4',
  size: 73400320,
  type: 'video/mp4',
  peerId: 'peer_xxx',
  peerName: 'My Device',
  roomCode: 'ABC123',
  transferId: 'transfer_xxx',
  totalChunks: 70,
  chunkSize: 1048576,
  status: 'pending|ready|transferring|completed|cancelled|expired',
  hash: 'optional-whole-file-hash',
  expiresAt: 1700000000000,
  uploadedAt: 1700000000000
}
```

## Operational notes

- Max file size defaults to **2 GB**
- Default chunk size is **1 MB**
- Max chunk size is **4 MB**
- In-flight chunk window defaults to **8**
- Per-transfer chunk buffer defaults to **64 MB**
- Transfer TTL defaults to **60 minutes**
- Cleanup runs every **30 seconds**
- Peer inactivity timeout defaults to **30 seconds**

## Frontend migration notes

- Remove all paths that expect `file.data` from Socket.IO.
- Slice uploads in the browser with `File.slice`.
- Fetch chunks progressively and acknowledge each chunk after local persistence/assembly.
- Use `get-transfer-state` after reconnect to resume missing chunks.

## License

MIT
