/**
 * FlatSQL Unified Worker
 * Works in: Browser Web Workers, Node.js worker_threads, Deno Workers
 *
 * Usage:
 *   Browser: new Worker('./flatsql.worker.js', { type: 'module' })
 *   Node:    new Worker('./flatsql.worker.js')
 *   Deno:    new Worker(new URL('./flatsql.worker.js', import.meta.url), { type: 'module' })
 */

// Environment detection
const isBrowser = typeof self !== 'undefined' && typeof self.postMessage === 'function';
const isNode = typeof process !== 'undefined' && process.versions && process.versions.node;
const isDeno = typeof Deno !== 'undefined';

// Message passing abstraction
let postMessage, onMessage;

if (isBrowser) {
    postMessage = (msg) => self.postMessage(msg);
    onMessage = (handler) => { self.onmessage = (e) => handler(e.data); };
} else if (isNode) {
    const { parentPort } = await import('worker_threads');
    postMessage = (msg) => parentPort.postMessage(msg);
    onMessage = (handler) => parentPort.on('message', handler);
} else if (isDeno) {
    postMessage = (msg) => self.postMessage(msg);
    onMessage = (handler) => { self.onmessage = (e) => handler(e.data); };
}

// WASM module loading
let flatsql = null;
let databases = new Map();

async function loadModule() {
    if (flatsql) return flatsql;

    // Dynamic import of the WASM module
    const FlatSQLModule = (await import('./flatsql.js')).default;
    flatsql = await FlatSQLModule();
    return flatsql;
}

// Helper: Convert Uint8Array to C++ vector
function toVec(arr) {
    const vec = new flatsql.VectorUint8();
    for (const byte of arr) vec.push_back(byte);
    return vec;
}

// Helper: Build size-prefixed stream from buffers
function buildStream(buffers) {
    const parts = [];
    for (const buf of buffers) {
        const size = new Uint8Array(4);
        new DataView(size.buffer).setUint32(0, buf.length, true);
        parts.push(size, new Uint8Array(buf));
    }
    const total = parts.reduce((s, p) => s + p.length, 0);
    const result = new Uint8Array(total);
    let offset = 0;
    for (const p of parts) {
        result.set(p, offset);
        offset += p.length;
    }
    return result;
}

// API Methods
const methods = {
    async init() {
        await loadModule();
        return { success: true, version: '1.0.0' };
    },

    async createDatabase({ id, schema, name = 'default' }) {
        await loadModule();
        const db = new flatsql.FlatSQLDatabase(schema, name);
        databases.set(id, { db, schema, name });
        return { success: true, id };
    },

    async registerFileId({ dbId, fileId, tableName }) {
        const entry = databases.get(dbId);
        if (!entry) throw new Error(`Database not found: ${dbId}`);
        entry.db.registerFileId(fileId, tableName);
        return { success: true };
    },

    async enableDemoExtractors({ dbId }) {
        const entry = databases.get(dbId);
        if (!entry) throw new Error(`Database not found: ${dbId}`);
        entry.db.enableDemoExtractors();
        return { success: true };
    },

    async setFieldExtractor({ dbId, tableName, extractorName }) {
        const entry = databases.get(dbId);
        if (!entry) throw new Error(`Database not found: ${dbId}`);
        // Note: Custom extractors need to be compiled into WASM
        // This is a placeholder for built-in extractors
        return { success: true };
    },

    async query({ dbId, sql }) {
        const entry = databases.get(dbId);
        if (!entry) throw new Error(`Database not found: ${dbId}`);

        const result = entry.db.query(sql);
        const response = {
            columns: result.getColumns(),
            rows: result.getRows(),
            rowCount: result.getRowCount()
        };
        result.delete();
        return response;
    },

    async ingest({ dbId, data }) {
        const entry = databases.get(dbId);
        if (!entry) throw new Error(`Database not found: ${dbId}`);

        const vec = toVec(new Uint8Array(data));
        const result = entry.db.ingest(vec);
        vec.delete();
        return { bytesConsumed: result };
    },

    async ingestOne({ dbId, data }) {
        const entry = databases.get(dbId);
        if (!entry) throw new Error(`Database not found: ${dbId}`);

        const vec = toVec(new Uint8Array(data));
        const rowId = entry.db.ingestOne(vec);
        vec.delete();
        return { rowId };
    },

    async ingestStream({ dbId, buffers }) {
        const entry = databases.get(dbId);
        if (!entry) throw new Error(`Database not found: ${dbId}`);

        const stream = buildStream(buffers);
        const vec = toVec(stream);
        const result = entry.db.ingest(vec);
        vec.delete();
        return { bytesConsumed: result, recordCount: buffers.length };
    },

    async exportData({ dbId }) {
        const entry = databases.get(dbId);
        if (!entry) throw new Error(`Database not found: ${dbId}`);

        const data = entry.db.exportData();
        return { data: Array.from(data) };
    },

    async listTables({ dbId }) {
        const entry = databases.get(dbId);
        if (!entry) throw new Error(`Database not found: ${dbId}`);
        return { tables: entry.db.listTables() };
    },

    async getStats({ dbId }) {
        const entry = databases.get(dbId);
        if (!entry) throw new Error(`Database not found: ${dbId}`);
        return { stats: entry.db.getStats() };
    },

    async deleteDatabase({ dbId }) {
        const entry = databases.get(dbId);
        if (!entry) throw new Error(`Database not found: ${dbId}`);
        entry.db.delete();
        databases.delete(dbId);
        return { success: true };
    },

    // Test helpers (for demo purposes)
    async createTestUser({ id, name, email, age }) {
        await loadModule();
        const fb = flatsql.createTestUser(id, name, email, age);
        return { data: Array.from(fb) };
    },

    async createTestPost({ id, userId, title }) {
        await loadModule();
        const fb = flatsql.createTestPost(id, userId, title);
        return { data: Array.from(fb) };
    }
};

// Message handler
onMessage(async (message) => {
    const { id, method, params = {} } = message;

    try {
        if (!methods[method]) {
            throw new Error(`Unknown method: ${method}`);
        }

        const result = await methods[method](params);
        postMessage({ id, success: true, result });
    } catch (error) {
        postMessage({ id, success: false, error: error.message, stack: error.stack });
    }
});

// Signal ready
postMessage({ type: 'ready' });
