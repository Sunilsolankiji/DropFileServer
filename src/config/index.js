import dotenv from 'dotenv';

dotenv.config();

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

const SOCKET_RATE_LIMIT = 60;
const SOCKET_RATE_WINDOW = 1000;

const corsOptions = {
  origin: NODE_ENV === 'development' ? '*' : ALLOWED_ORIGINS,
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'x-peer-id', 'x-chunk-hash'],
  exposedHeaders: ['Content-Length', 'X-Chunk-Index', 'X-Chunk-Hash']
};

export const config = {
  PORT,
  NODE_ENV,
  ALLOWED_ORIGINS,
  PUBLIC_BASE_URL,
  MAX_FILE_SIZE,
  FILE_TTL_MS,
  CLEANUP_INTERVAL,
  MAX_FILES_PER_ROOM,
  INACTIVE_TIMEOUT,
  DEFAULT_CHUNK_SIZE,
  MAX_CHUNK_SIZE,
  MAX_INFLIGHT_CHUNKS,
  MAX_TRANSFER_BUFFER_BYTES,
  SOCKET_RATE_LIMIT,
  SOCKET_RATE_WINDOW,
  corsOptions
};