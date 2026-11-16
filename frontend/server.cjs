const fs = require('node:fs');
const http = require('node:http');
const path = require('node:path');
const next = require('next');

const root = path.resolve(__dirname, '..');
const runtimeDir = path.join(root, '.runtime');
const stopFile = path.join(runtimeDir, 'frontend.stop');
const port = 3000;

async function start() {
  fs.mkdirSync(runtimeDir, { recursive: true });
  fs.rmSync(stopFile, { force: true });

  const app = next({ dev: true, hostname: '127.0.0.1', port, dir: __dirname });
  await app.prepare();
  const handle = app.getRequestHandler();
  const upgrade = app.getUpgradeHandler();
  const sockets = new Set();
  const server = http.createServer((request, response) => {
    handle(request, response).catch((error) => {
      app.logError(error);
      if (!response.headersSent) response.writeHead(500);
      response.end('Frontend request failed');
    });
  });
  server.on('connection', (socket) => {
    sockets.add(socket);
    socket.once('close', () => sockets.delete(socket));
  });
  server.on('upgrade', (request, socket, head) => {
    upgrade(request, socket, head).catch((error) => {
      app.logError(error);
      socket.destroy();
    });
  });

  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', () => {
      server.removeListener('error', reject);
      resolve();
    });
  });
  console.log('Frontend ready at http://127.0.0.1:3000');

  let stopping;
  const stop = () => {
    if (stopping) return stopping;
    stopping = (async () => {
      clearInterval(stopPoll);
      const closed = new Promise((resolve, reject) => {
        server.close((error) => error ? reject(error) : resolve());
      });
      // Next dev keeps HMR WebSockets upgraded outside the HTTP server's close tracking.
      for (const socket of sockets) socket.end();
      const forceCloseTimer = setTimeout(() => {
        for (const socket of sockets) socket.destroy();
        server.closeAllConnections?.();
      }, 2000);
      await app.close();
      await closed;
      clearTimeout(forceCloseTimer);
      fs.rmSync(stopFile, { force: true });
      console.log('Frontend stopped cleanly');
      process.exit(0);
    })();
    return stopping;
  };
  const stopPoll = setInterval(() => {
    if (fs.existsSync(stopFile)) {
      stop().catch((error) => {
        console.error('Frontend shutdown failed:', error);
        process.exitCode = 1;
      });
    }
  }, 250);
  stopPoll.unref();
  process.once('SIGINT', () => { stop().catch((error) => { console.error(error); process.exitCode = 1; }); });
  process.once('SIGTERM', () => { stop().catch((error) => { console.error(error); process.exitCode = 1; }); });
}

start().catch((error) => {
  console.error('Frontend startup failed:', error);
  process.exitCode = 1;
});