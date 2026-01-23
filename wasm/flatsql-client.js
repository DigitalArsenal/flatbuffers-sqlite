/**
 * FlatSQL Worker Client
 * Provides a clean async API for communicating with the FlatSQL worker.
 * Works in: Browser, Node.js, Deno
 *
 * Usage:
 *   import { FlatSQLClient, FlatSQLWorkerDatabase } from './flatsql-client.js';
 *
 *   const client = new FlatSQLClient();
 *   await client.init();
 *
 *   const db = await client.createDatabase(schema, 'mydb');
 *   await db.registerFileId('USER', 'User');
 *   const result = await db.query('SELECT * FROM User');
 */

// Environment detection
const isBrowser = typeof window !== 'undefined' || (typeof self !== 'undefined' && typeof importScripts === 'undefined');
const isNode = typeof process !== 'undefined' && process.versions && process.versions.node;
const isDeno = typeof Deno !== 'undefined';

/**
 * Create a worker based on the environment
 */
async function createWorker(workerPath) {
    if (isNode) {
        const { Worker } = await import('worker_threads');
        return new Worker(workerPath);
    } else if (isDeno) {
        return new Worker(new URL(workerPath, import.meta.url), { type: 'module' });
    } else {
        // Browser
        return new Worker(workerPath, { type: 'module' });
    }
}

/**
 * Setup message handlers for the worker
 */
function setupWorkerHandlers(worker, handlers) {
    if (isNode) {
        worker.on('message', handlers.onMessage);
        worker.on('error', handlers.onError);
        worker.on('exit', handlers.onExit);
    } else {
        worker.onmessage = (e) => handlers.onMessage(e.data);
        worker.onerror = handlers.onError;
    }
}

/**
 * FlatSQL Worker Client
 * Manages communication with the FlatSQL worker.
 */
export class FlatSQLClient {
    constructor(workerPath = './flatsql.worker.js') {
        this.workerPath = workerPath;
        this.worker = null;
        this.pending = new Map();
        this.nextId = 1;
        this.ready = false;
        this.readyPromise = null;
        this.databases = new Map();
    }

    /**
     * Initialize the client and worker
     */
    async init() {
        if (this.worker) return this;

        this.worker = await createWorker(this.workerPath);

        this.readyPromise = new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
                reject(new Error('Worker initialization timeout'));
            }, 30000);

            const handlers = {
                onMessage: (data) => {
                    if (data.type === 'ready') {
                        clearTimeout(timeout);
                        this.ready = true;
                        resolve();
                        return;
                    }

                    const { id, success, result, error } = data;
                    const pending = this.pending.get(id);
                    if (pending) {
                        this.pending.delete(id);
                        if (success) {
                            pending.resolve(result);
                        } else {
                            pending.reject(new Error(error));
                        }
                    }
                },
                onError: (err) => {
                    clearTimeout(timeout);
                    reject(err);
                },
                onExit: (code) => {
                    if (code !== 0) {
                        reject(new Error(`Worker exited with code ${code}`));
                    }
                }
            };

            setupWorkerHandlers(this.worker, handlers);
        });

        await this.readyPromise;

        // Initialize WASM in worker
        await this.call('init');

        return this;
    }

    /**
     * Call a method on the worker
     */
    async call(method, params = {}) {
        if (!this.worker) {
            throw new Error('Client not initialized. Call init() first.');
        }

        const id = this.nextId++;

        return new Promise((resolve, reject) => {
            this.pending.set(id, { resolve, reject });
            this.worker.postMessage({ id, method, params });
        });
    }

    /**
     * Create a new database
     */
    async createDatabase(schema, name = 'default') {
        const dbId = `db_${this.nextId++}`;
        await this.call('createDatabase', { id: dbId, schema, name });
        const db = new FlatSQLWorkerDatabase(this, dbId);
        this.databases.set(dbId, db);
        return db;
    }

    /**
     * Create a test User FlatBuffer (for demos)
     */
    async createTestUser(id, name, email, age) {
        const result = await this.call('createTestUser', { id, name, email, age });
        return new Uint8Array(result.data);
    }

    /**
     * Create a test Post FlatBuffer (for demos)
     */
    async createTestPost(id, userId, title) {
        const result = await this.call('createTestPost', { id, userId, title });
        return new Uint8Array(result.data);
    }

    /**
     * Terminate the worker
     */
    terminate() {
        if (this.worker) {
            if (isNode) {
                this.worker.terminate();
            } else {
                this.worker.terminate();
            }
            this.worker = null;
            this.ready = false;
        }
    }
}

/**
 * FlatSQL Database accessed through a worker
 */
export class FlatSQLWorkerDatabase {
    constructor(client, dbId) {
        this._client = client;
        this._dbId = dbId;
    }

    /**
     * Register a file ID for routing FlatBuffers to tables
     */
    async registerFileId(fileId, tableName) {
        await this._client.call('registerFileId', {
            dbId: this._dbId,
            fileId,
            tableName
        });
    }

    /**
     * Enable built-in demo extractors (User/Post)
     */
    async enableDemoExtractors() {
        await this._client.call('enableDemoExtractors', { dbId: this._dbId });
    }

    /**
     * Execute a SQL query
     */
    async query(sql) {
        return await this._client.call('query', { dbId: this._dbId, sql });
    }

    /**
     * Ingest a size-prefixed FlatBuffer stream
     */
    async ingest(data) {
        const result = await this._client.call('ingest', {
            dbId: this._dbId,
            data: Array.from(data)
        });
        return result.bytesConsumed;
    }

    /**
     * Ingest a single FlatBuffer (without size prefix)
     */
    async ingestOne(data) {
        const result = await this._client.call('ingestOne', {
            dbId: this._dbId,
            data: Array.from(data)
        });
        return result.rowId;
    }

    /**
     * Ingest multiple FlatBuffers (builds stream internally)
     */
    async ingestStream(buffers) {
        const result = await this._client.call('ingestStream', {
            dbId: this._dbId,
            buffers: buffers.map(b => Array.from(b))
        });
        return result;
    }

    /**
     * Export database as binary blob
     */
    async exportData() {
        const result = await this._client.call('exportData', { dbId: this._dbId });
        return new Uint8Array(result.data);
    }

    /**
     * List all tables
     */
    async listTables() {
        const result = await this._client.call('listTables', { dbId: this._dbId });
        return result.tables;
    }

    /**
     * Get database statistics
     */
    async getStats() {
        const result = await this._client.call('getStats', { dbId: this._dbId });
        return result.stats;
    }

    /**
     * Delete the database and free resources
     */
    async delete() {
        await this._client.call('deleteDatabase', { dbId: this._dbId });
        this._client.databases.delete(this._dbId);
    }
}

export default FlatSQLClient;
