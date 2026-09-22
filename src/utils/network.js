import os from 'os';
import { config } from '../config/index.js';

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

export const LOCAL_IP = getLocalIP();

export function getPublicBaseUrl() {
  return config.PUBLIC_BASE_URL || `http://${LOCAL_IP}:${config.PORT}`;
}

export function getTransferChunkUrlTemplate(transferId) {
  const path = `/api/transfers/${transferId}/chunks/{chunkIndex}`;
  return config.PUBLIC_BASE_URL ? `${config.PUBLIC_BASE_URL}${path}` : path;
}

export function getServerInfo() {
  return {
    ip: LOCAL_IP,
    port: config.PORT,
    publicBaseUrl: getPublicBaseUrl()
  };
}