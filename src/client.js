'use strict';

const net = require('net');

/**
 * Parse a raw server response into a clean result object.
 * The server returns lines like:
 *   OK 1 row pushed
 *   OK [{...},{...}]
 *   OK 5
 *   ERR table "x" not found
 */
function parseResponse(raw) {
  raw = raw.trim();

  if (raw.startsWith('ERR ')) {
    const err = new Error(raw.slice(4));
    err.code = 'EVODB_ERR';
    throw err;
  }

  if (!raw.startsWith('OK')) {
    throw new Error('Unexpected response: ' + raw);
  }

  const body = raw.slice(2).trim(); // strip "OK"

  // Empty / no-table result
  if (body === '' || body === '[]') return [];

  // Rows result: OK [{...},...]
  if (body.startsWith('[')) {
    try {
      return JSON.parse(body);
    } catch {
      return body;
    }
  }

  // Count: OK 42
  if (/^\d+$/.test(body)) return Number(body);

  // "1 row pushed", "upserted", "N row(s) burned", table names, schema, etc.
  return body;
}

class EvoConnection {
  constructor(host, port) {
    this.host = host || 'localhost';
    this.port = port || 7777;
    this._socket = null;
    this._reader = '';
    this._queue = [];
    this._ready = false;
  }

  connect() {
    return new Promise((resolve, reject) => {
      const sock = net.createConnection(this.port, this.host);
      this._socket = sock;

      sock.setEncoding('utf8');

      sock.on('data', (chunk) => {
        this._reader += chunk;
        const lines = this._reader.split('\n');
        this._reader = lines.pop(); // keep incomplete last line

        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed) continue;

          // First line is the welcome banner
          if (!this._ready) {
            if (trimmed.startsWith('LURUS') || trimmed.startsWith('EVODB')) {
              this._ready = true;
              resolve(this);
            } else {
              reject(new Error('Bad handshake: ' + trimmed));
            }
            continue;
          }

          // Resolve the oldest pending query
          if (this._queue.length > 0) {
            const { resolve: res, reject: rej } = this._queue.shift();
            try {
              res(parseResponse(trimmed));
            } catch (e) {
              rej(e);
            }
          }
        }
      });

      sock.on('error', reject);
      sock.on('close', () => {
        this._ready = false;
        // Reject any pending
        for (const { reject: rej } of this._queue) {
          rej(new Error('Connection closed'));
        }
        this._queue = [];
      });
    });
  }

  send(cmd) {
    return new Promise((resolve, reject) => {
      if (!this._socket || !this._ready) {
        return reject(new Error('Not connected'));
      }
      this._queue.push({ resolve, reject });
      this._socket.write(cmd + '\n');
    });
  }

  close() {
    if (this._socket) this._socket.destroy();
  }
}

/**
 * Connection pool — reuses TCP connections.
 */
class Pool {
  constructor(host, port, size = 8) {
    this.host = host;
    this.port = port;
    this.size = size;
    this._free = [];
    this._waiters = [];
    this._total = 0;
  }

  async _acquire() {
    if (this._free.length > 0) return this._free.pop();
    if (this._total < this.size) {
      this._total++;
      const c = new EvoConnection(this.host, this.port);
      await c.connect();
      return c;
    }
    // Wait for a free connection
    return new Promise((resolve) => this._waiters.push(resolve));
  }

  _release(conn) {
    if (this._waiters.length > 0) {
      const waiter = this._waiters.shift();
      waiter(conn);
    } else {
      this._free.push(conn);
    }
  }

  async send(cmd) {
    const conn = await this._acquire();
    try {
      const result = await conn.send(cmd);
      this._release(conn);
      return result;
    } catch (err) {
      conn.close();
      this._total--;
      throw err;
    }
  }

  close() {
    for (const c of this._free) c.close();
    this._free = [];
    this._total = 0;
  }
}

/**
 * EvoDB client — the main class you use.
 *
 * @example
 * const { EvoDB } = require('evodb');
 * const db = new EvoDB(); // defaults: localhost:7777
 * await db.connect();
 *
 * await db.forge('users', { id: 'INT INDEX', name: 'STRING', active: 'BOOL' });
 * await db.push('users', [1, 'Alice', true]);
 * const rows = await db.pull('users');
 * // rows => [{ id: '1', name: 'Alice', active: 'true' }]
 */
class EvoDB {
  /**
   * @param {object} [opts]
   * @param {string} [opts.host='localhost']
   * @param {number} [opts.port=7777]
   * @param {number} [opts.pool=8]   Max connections in pool
   */
  constructor(opts = {}) {
    this.host = opts.host || 'localhost';
    this.port = opts.port || 7777;
    this._pool = new Pool(this.host, this.port, opts.pool || 8);
    this._connected = false;
  }

  /** Connect and warm up one connection. */
  async connect() {
    await this._pool.send('TABLES'); // ping to verify connectivity
    this._connected = true;
    return this;
  }

  /** Close all pooled connections. */
  close() {
    this._pool.close();
    this._connected = false;
  }

  /** Raw command — returns parsed result. */
  raw(cmd) {
    return this._pool.send(cmd);
  }

  // ─── Table management ──────────────────────────────────────────────────────

  /**
   * Create a table.
   * @param {string} table
   * @param {Record<string, string>} schema  e.g. { id: 'INT INDEX', name: 'STRING' }
   */
  forge(table, schema) {
    const cols = Object.entries(schema)
      .map(([name, type]) => `${name} ${type}`)
      .join(', ');
    return this._pool.send(`FORGE ${table} (${cols})`);
  }

  /**
   * Drop a table.
   * @param {string} table
   */
  drop(table) {
    return this._pool.send(`DROP ${table}`);
  }

  /**
   * List all tables.
   * @returns {Promise<string[]>}
   */
  async tables() {
    const res = await this._pool.send('TABLES');
    if (res === '(no tables)') return [];
    if (typeof res === 'string') return res.split(', ').map(s => s.trim());
    return res;
  }

  /**
   * Get table schema.
   * @param {string} table
   * @returns {Promise<Array<{name:string, type:string, indexed:boolean}>>}
   */
  async schema(table) {
    const res = await this._pool.send(`SCHEMA ${table}`);
    if (typeof res !== 'string') return [];
    return res.split(', ').map(part => {
      const tokens = part.trim().split(' ');
      return {
        name: tokens[0],
        type: tokens[1],
        indexed: tokens[2] === 'INDEX',
      };
    });
  }

  /**
   * Add an index to a column.
   * @param {string} table
   * @param {string} col
   */
  index(table, col) {
    return this._pool.send(`INDEX ${table} ON ${col}`);
  }

  // ─── Writing ───────────────────────────────────────────────────────────────

  /**
   * Insert a new row (always appends).
   * @param {string} table
   * @param {Array<string|number|boolean|null>} values
   */
  push(table, values) {
    return this._pool.send(`PUSH ${table} (${serializeValues(values)})`);
  }

  /**
   * Insert or update a row, matching on keyCol.
   * @param {string} table
   * @param {string} keyCol
   * @param {Array<string|number|boolean|null>} values
   */
  upsert(table, keyCol, values) {
    return this._pool.send(`UPSERT ${table} KEY ${keyCol} (${serializeValues(values)})`);
  }

  /**
   * Update rows matching a condition.
   * @param {string} table
   * @param {{ col: string, val: any }} where
   * @param {{ col: string, val: any }} set
   * @returns {Promise<number>} rows updated
   */
  async reforge(table, where, set) {
    const cmd = `REFORGE ${table} SET ${set.col} = ${serializeValue(set.val)} WHERE ${where.col} = ${serializeValue(where.val)}`;
    const res = await this._pool.send(cmd);
    const m = String(res).match(/^(\d+)/);
    return m ? Number(m[1]) : 0;
  }

  /**
   * Delete rows matching a condition.
   * @param {string} table
   * @param {string} col
   * @param {any} val
   * @returns {Promise<number>} rows deleted
   */
  async burn(table, col, val) {
    const res = await this._pool.send(`BURN ${table} WHERE ${col} = ${serializeValue(val)}`);
    const m = String(res).match(/^(\d+)/);
    return m ? Number(m[1]) : 0;
  }

  // ─── Reading ───────────────────────────────────────────────────────────────

  /**
   * Fetch rows from a table.
   * @param {string} table
   * @param {object} [opts]
   * @param {Array<{col:string, op?:string, val:any}>} [opts.where]   conditions (AND)
   * @param {{col:string, dir?:'ASC'|'DESC'}} [opts.orderBy]
   * @param {number} [opts.limit]
   * @returns {Promise<Array<Record<string,string>>>}
   */
  async pull(table, opts = {}) {
    let cmd = `PULL ${table}`;

    if (opts.where && opts.where.length > 0) {
      const conditions = opts.where
        .map(c => `${c.col} ${c.op || '='} ${serializeValue(c.val)}`)
        .join(' AND ');
      cmd += ` WHERE ${conditions}`;
    }

    if (opts.orderBy) {
      cmd += ` ORDER BY ${opts.orderBy.col} ${opts.orderBy.dir || 'ASC'}`;
    }

    if (opts.limit) {
      cmd += ` LIMIT ${opts.limit}`;
    }

    const rows = await this._pool.send(cmd);
    return Array.isArray(rows) ? rows : [];
  }

  /**
   * Shortcut: pull rows where col = val.
   * @param {string} table
   * @param {string} col
   * @param {any} val
   * @param {number} [limit]
   * @returns {Promise<Array<Record<string,string>>>}
   */
  pullWhere(table, col, val, limit) {
    const opts = { where: [{ col, val }] };
    if (limit) opts.limit = limit;
    return this.pull(table, opts);
  }

  /**
   * Count rows, optionally filtered.
   * @param {string} table
   * @param {string} [col]
   * @param {any} [val]
   * @returns {Promise<number>}
   */
  async count(table, col, val) {
    let cmd = `COUNT ${table}`;
    if (col !== undefined && val !== undefined) {
      cmd += ` WHERE ${col} = ${serializeValue(val)}`;
    }
    const res = await this._pool.send(cmd);
    return typeof res === 'number' ? res : Number(res);
  }
}

// ─── Value serializers ──────────────────────────────────────────────────────

function serializeValue(v) {
  if (v === null || v === undefined) return 'NULL';
  if (typeof v === 'boolean') return v ? 'true' : 'false';
  if (typeof v === 'number') return String(v);
  if (typeof v === 'string') {
    // JSON objects/arrays pass through raw
    const t = v.trim();
    if ((t.startsWith('{') && t.endsWith('}')) || (t.startsWith('[') && t.endsWith(']'))) {
      return t;
    }
    return `"${v.replace(/"/g, '\\"')}"`;
  }
  if (typeof v === 'object') return JSON.stringify(v);
  return String(v);
}

function serializeValues(arr) {
  return arr.map(serializeValue).join(', ');
}

module.exports = { EvoDB, EvoConnection };
