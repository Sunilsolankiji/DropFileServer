import { log } from './logger.js';

export function safeCallback(callback, response) {
  if (typeof callback === 'function') {
    try {
      callback(response);
    } catch (err) {
      log('error', 'Callback error', { error: err.message });
    }
  }
}