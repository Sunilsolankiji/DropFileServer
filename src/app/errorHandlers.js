import { config } from '../config/index.js';

export function notFoundHandler(req, res) {
  res.status(404).json({
    error: 'Not found',
    path: req.path
  });
}

export function errorHandler(err, req, res, next) {
  console.error('Error:', err);
  res.status(500).json({
    error: 'Internal server error',
    message: config.NODE_ENV === 'development' ? err.message : undefined
  });
}