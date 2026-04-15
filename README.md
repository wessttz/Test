# evodb

Node.js client for **EvoDB** — a fast, lightweight custom database with its own binary protocol. Built for bots and microservices. No ORM bloat, no JSON escaping hell.

---

## Setup

### 1. Install the npm package

```bash
npm install evodb
```

### 2. Compile & run the server

You need [Go](https://go.dev/dl/) installed once to build the binary:

```bash
node node_modules/evodb/scripts/build.js   # compiles bin/evodb
```

Then start the server:

```bash
./node_modules/evodb/bin/evodb serve mybot.evodb
```

Or copy the binary to your PATH and run from anywhere:

```bash
evodb serve mybot.evodb          # default port :7777
evodb serve mybot.evodb :9000    # custom port
```

---

## Usage

```js
const { EvoDB } = require('evodb');

const db = new EvoDB({ host: 'localhost', port: 7777 });
await db.connect();

// Create a table
await db.forge('users', {
  id:     'INT INDEX',
  name:   'STRING',
  xp:     'INT',
  active: 'BOOL',
});

// Insert a row
await db.push('users', [1, 'Alice', 500, true]);

// Upsert (insert or update by key)
await db.upsert('users', 'id', [1, 'Alice', 750, true]);

// Fetch all rows
const users = await db.pull('users');
// => [{ id: '1', name: 'Alice', xp: '750', active: 'true' }]

// Filter rows
const alice = await db.pullWhere('users', 'name', 'Alice');

// Advanced query
const top = await db.pull('users', {
  where:   [{ col: 'active', val: true }],
  orderBy: { col: 'xp', dir: 'DESC' },
  limit:   10,
});

// Count
const total = await db.count('users');             // all rows
const active = await db.count('users', 'active', true); // filtered

// Update rows
const updated = await db.reforge('users',
  { col: 'id', val: 1 },       // WHERE
  { col: 'xp', val: 1000 }     // SET
);
// => 1 (number of rows updated)

// Delete rows
const deleted = await db.burn('users', 'id', 1);
// => 1 (number of rows deleted)

// List tables
const tables = await db.tables();
// => ['users', ...]

// Schema
const schema = await db.schema('users');
// => [{ name: 'id', type: 'INT', indexed: true }, ...]

// Drop a table
await db.drop('users');

db.close();
```

---

## Data types

| EvoDB type | JS value examples |
|------------|------------------|
| `INT`      | `42`, `-7` |
| `FLOAT`    | `3.14` |
| `STRING`   | `'hello'` |
| `BOOL`     | `true`, `false` |
| `JSON`     | `{ any: 'object' }` stored as raw JSON string |
| `NULL`     | `null` |

---

## EvoDB CLI (REPL)

```bash
evodb repl
evodb repl localhost:9000
```

Commands in the REPL:

```
FORGE users (id INT INDEX, name STRING, xp INT)
PUSH users (1, "Alice", 500)
UPSERT users KEY id (1, "Alice", 750)
PULL users
PULL users WHERE xp > 100 ORDER BY xp DESC LIMIT 5
COUNT users WHERE active = true
REFORGE users SET xp = 1000 WHERE id = 1
BURN users WHERE id = 1
TABLES
SCHEMA users
DROP users
```

---

## Constructor options

| Option | Default | Description |
|--------|---------|-------------|
| `host` | `'localhost'` | Server hostname |
| `port` | `7777` | TCP port |
| `pool` | `8` | Max pooled connections |

---

## Why not PostgreSQL?

EvoDB uses a compact binary file format (`.evodb`) with a WAL for crash safety. It has zero external dependencies, starts instantly, and is tiny. Perfect for Discord bots, Telegram bots, or any small service that needs persistent structured storage without running a full Postgres instance.

---

## Architecture

```
your bot (Node.js)
    │
    │  TCP :7777
    ▼
evodb server (Go binary)
    │
    ├── WAL  (mydb.evodb.wal)
    └── Data (mydb.evodb)
```

The server also exposes:
- `HTTP POST /query`  — send commands over HTTP
- `HTTP GET  /ping`   — status dashboard (browser-friendly)
- `HTTP GET  /api/status` — JSON status
