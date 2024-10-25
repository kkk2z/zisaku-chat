const express = require('express');
const { createServer } = require('node:http');
const { join } = require('node:path');
const { Server } = require('socket.io');
const sqlite3 = require('sqlite3');
const { open } = require('sqlite');
const { availableParallelism } = require('node:os');
const cluster = require('node:cluster');
const { createAdapter, setupPrimary } = require('@socket.io/cluster-adapter');

if (cluster.isPrimary) {
  const numCPUs = availableParallelism();
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork({
      PORT: 3000 + i
    });
  }
  return setupPrimary();
}

async function main() {
  const db = await open({
    filename: 'chat.db',
    driver: sqlite3.Database
  });

  await db.exec(`
    CREATE TABLE IF NOT EXISTS messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      client_offset TEXT UNIQUE,
      content TEXT
    );
  `);

  const app = express();
  const server = createServer(app);
  const io = new Server(server, {
    connectionStateRecovery: {},
    adapter: createAdapter()
  });

  app.get('/', (req, res) => {
    res.sendFile(join(__dirname, 'index.html'));
  });

  io.on('connection', (socket) => {
    socket.on('set nickname', (nickname) => {
      socket.nickname = nickname; // ニックネームを保存
    });

    socket.on('chat message', async (msg, clientOffset, callback = () => {}) => {
      const fullMessage = `${socket.nickname || 'Anonymous'}: ${msg}`; // ニックネームを含める
      let result;
      try {
        result = await db.run('INSERT INTO messages (content, client_offset) VALUES (?, ?)', fullMessage, clientOffset);
        io.emit('chat message', fullMessage, result.lastID); // メッセージをブロードキャスト
      } catch (e) {
        if (e.errno === 19) { // SQLITE_CONSTRAINT
          callback();
        }
        return;
      }
      callback(); // コールバックを呼び出す
    });
  });

  const port = process.env.PORT || 3000;

  server.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
  });
}

main();
