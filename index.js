const express = require('express')
const { createServer } = require('node:http')
const { join } = require('node:path')
const { Server } = require('socket.io')
const sqlite3 = require('sqlite3');
const { open } = require('sqlite');
const { availableParallelism } = require('node:os')
const cluster = require("node:cluster")
const { createAdapter, setupPrimary } = require('@socket.io/cluster-adapter')

if (cluster.isPrimary) {
  const numCPUs = availableParallelism()

  for (let i = 0; i < numCPUs; i++) {
    cluster.fork({
      PORT: 3000 + i
    })
  }

  return setupPrimary()
}

async function main() {
    // open the database file
    const db = await open({
      filename: 'chat.db',
      driver: sqlite3.Database
    });
  
    // create our 'messages' table (you can ignore the 'client_offset' column for now)
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
  
    io.on('connection', async (socket) => {
      socket.on('chat message', async (msg, cliestOffset, callback) => {
        let result;
        try {
          // store the message in the database
          result = await db.run('INSERT INTO messages (content, client_offset) VALUES (?, ?)', msg, cliestOffset);
        } catch (e) {
          if (e.errno === 19) {
            callback()
          } else {

          }
          return;
        }
        // include the offset with the message
        io.emit('chat message', msg, result.lastID);
        callback()
      });
      if (!socket.recovered) {
        // if the connection state recovery was not successful
        try {
          await db.each('SELECT id, content FROM messages WHERE id > ?',
            [socket.handshake.auth.serverOffset || 0],
            (_err, row) => {
              socket.emit('chat message', row.content, row.id);
            }
          )
        } catch (e) {
          // something went wrong
        }
      }
    });

    const port = process.env.PORT
  
    server.listen(port, () => {
      console.log(`server running at http://localhost:${port}`);
    });
  }

main()