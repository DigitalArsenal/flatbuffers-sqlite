# FlatSQL

**SQL queries over raw FlatBuffer storage** — A streaming query engine that keeps data in native FlatBuffer format while providing SQL access via SQLite virtual tables.

## What is FlatSQL?

FlatSQL bridges two technologies:

- **[FlatBuffers](https://github.com/digitalarsenal/flatbuffers)** — Google's efficient cross-platform serialization library. Data is stored in binary format with zero-copy access (no parsing/unpacking needed).
- **SQLite** — The most widely deployed SQL database engine, used here only for SQL parsing and query execution.

The key insight: instead of converting FlatBuffers to SQLite rows (expensive), FlatSQL uses [SQLite virtual tables](https://sqlite.org/vtab.html) to query FlatBuffer data directly. Your data stays in portable FlatBuffer format, readable by any FlatBuffer tooling, while you get SQL query capabilities.

## Live Demo

Try FlatSQL in your browser: **[https://digitalarsenal.github.io/flatbuffers-sqlite/](https://digitalarsenal.github.io/flatbuffers-sqlite/)**

## Source Code

| Repository | Description |
|------------|-------------|
| [digitalarsenal/flatbuffers-sqlite](https://github.com/digitalarsenal/flatbuffers-sqlite) | This project — FlatSQL query engine |
| [digitalarsenal/flatbuffers](https://github.com/digitalarsenal/flatbuffers) | Fork of Google FlatBuffers with WASM support |
| [flatc-wasm](https://digitalarsenal.github.io/flatbuffers/) | FlatBuffer compiler running in WebAssembly |

## Why FlatSQL?

Traditional approach:
```
FlatBuffer → Deserialize → SQLite rows → Query → Serialize → FlatBuffer
```

FlatSQL approach:
```
FlatBuffer → Query (via virtual table) → FlatBuffer
```

**Benefits:**
- **Zero conversion overhead** — Data stays in FlatBuffer format
- **Streaming ingestion** — Indexes built during data arrival, not after
- **Portable output** — Exported data is standard FlatBuffers, readable by any tooling
- **Multi-source federation** — Query across multiple FlatBuffer sources with automatic source tagging

## Modes of Operation

### 1. WASM (Browser/Node.js)

The C++ engine compiles to WebAssembly for cross-platform deployment:

```javascript
import { createFlatSQL } from './wasm/index.js';

const flatsql = await createFlatSQL();

// Register your schema
flatsql.registerSchema(`
  table User {
    id: int (key);
    name: string;
    email: string (indexed);
  }
`);

// Ingest FlatBuffer data (streaming)
flatsql.ingest(flatbufferBytes);

// Query with SQL
const result = flatsql.query('SELECT * FROM User WHERE id = 42');
console.log(result.columns, result.rows);
```

### 2. WASM Worker (Browser/Node.js/Deno)

For offloading database operations to a background thread, use the unified worker API:

```javascript
import { FlatSQLClient } from './wasm/flatsql-client.js';

// Initialize client (spawns worker)
const client = new FlatSQLClient('./flatsql.worker.js');
await client.init();

// Create a database
const schema = `
  table User {
    id: int (id);
    name: string;
    email: string (key);
    age: int;
  }
`;
const db = await client.createDatabase(schema, 'myapp');
await db.registerFileId('USER', 'User');

// Ingest FlatBuffer data
const bytesConsumed = await db.ingest(flatbufferStream);

// Query
const result = await db.query('SELECT * FROM User WHERE age > 25');
console.log(result.columns, result.rows);

// Export database
const exported = await db.exportData();

// Cleanup
client.terminate();
```

The worker automatically detects the runtime environment:

- **Browser**: Uses Web Workers with `postMessage`
- **Node.js**: Uses `worker_threads` with `parentPort`
- **Deno**: Uses Deno Workers with `postMessage`

### 3. TypeScript (Pure JavaScript)

A TypeScript implementation for environments where WASM isn't available or for development:

```typescript
import { FlatSQLDatabase, FlatcAccessor } from 'flatbuffers-sqlite';

const schema = `
  namespace App;

  table User {
    id: int (key);
    name: string (required);
    email: string;
    age: int;
  }
`;

const accessor = new FlatcAccessor(flatc, schema);
const db = FlatSQLDatabase.fromSchema(schema, accessor, 'myapp');

// Insert records
db.insert('User', { id: 1, name: 'Alice', email: 'alice@example.com', age: 30 });
db.insert('User', { id: 2, name: 'Bob', email: 'bob@example.com', age: 25 });

// Query
const result = db.query('SELECT name, email FROM User WHERE age > 20');
console.log(result.rows);

// Export as standard FlatBuffers
const exported = db.exportData();
```

### 4. Native C++ (Embedded)

For performance-critical applications, link the C++ library directly:

```cpp
#include <flatsql/database.h>

auto db = flatsql::FlatSQLDatabase::fromSchema(schema);

// Register file ID routing
db.registerFileId("USER", "users_table");

// Ingest streaming data
size_t consumed = db.ingest(data, length, &recordsIngested);

// Query
auto result = db.query("SELECT * FROM users_table WHERE id = 5");
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     FlatSQLDatabase                          │
├─────────────────────────────────────────────────────────────┤
│   SchemaParser        │      SQLiteEngine                    │
│   (FlatBuffers IDL)   │      (Virtual Tables)                │
├─────────────────────────────────────────────────────────────┤
│                    TableStore (per table)                    │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  B-Tree Indexes            Field Extractors             │ │
│  │  (id, email, timestamp)    (getField callbacks)         │ │
│  └────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│              StackedFlatBufferStore (append-only)            │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ [Header][FB₁][FB₂][FB₃]...                           │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Storage Format

Data is stored as "stacked FlatBuffers" — a simple append-only format:

```
[File Header: 64 bytes]
  - Magic: "FLSQ"
  - Version: 1
  - Data offset, record count, schema name

[Record Header: 48 bytes][FlatBuffer]
[Record Header: 48 bytes][FlatBuffer]
...
```

Each FlatBuffer is complete and valid. You can:
- Read individual records with standard FlatBuffer code
- Seek to any record by offset
- Append without rewriting existing data
- Process the file with any FlatBuffer tooling

### Query Flow

```
SQL Query
    ↓
SQLite Parser
    ↓
Virtual Table (xFilter)
    ↓
┌─────────────────────────────┐
│ Index lookup or full scan   │
│ via B-tree                  │
└─────────────────────────────┘
    ↓
Field Extraction (per row)
    ↓
Result Set
```

## Examples

### Basic Usage

```typescript
import { FlatSQLDatabase } from 'flatbuffers-sqlite';

const schema = `
  namespace Analytics;

  table Event {
    id: long (key);
    user_id: int (indexed);
    type: string (indexed);
    payload: string;
    timestamp: long (indexed);
  }
`;

const db = FlatSQLDatabase.fromSchema(schema, accessor, 'analytics');

// Stream events
for await (const event of eventStream) {
  db.insert('Event', event);
}

// Query recent events for a user
const result = db.query(`
  SELECT type, payload, timestamp
  FROM Event
  WHERE user_id = 12345
  ORDER BY timestamp DESC
  LIMIT 100
`);
```

### Multi-Source Federation

```cpp
// Register multiple data sources
engine.registerSource("logs_2024", store2024, eventDef, "EVNT");
engine.registerSource("logs_2025", store2025, eventDef, "EVNT");

// Create unified view
engine.createUnifiedView("all_logs", {"logs_2024", "logs_2025"});

// Query across all sources (automatic _source column)
auto result = engine.execute(
  "SELECT _source, type, COUNT(*) FROM all_logs GROUP BY _source, type"
);
```

### Streaming Ingestion

```typescript
// Stream generator
async function* eventGenerator() {
  for (let i = 0; i < 100000; i++) {
    yield {
      id: i,
      type: ['click', 'view', 'purchase'][i % 3],
      timestamp: Date.now()
    };
  }
}

// Ingest with progress
let count = 0;
for await (const event of eventGenerator()) {
  db.insert('Event', event);
  if (++count % 10000 === 0) {
    console.log(`Ingested ${count} events`);
  }
}
```

## Building

### Prerequisites

- Node.js 18+
- CMake 3.20+ (for native/WASM builds)
- C++17 compiler (for native builds)
- Emscripten (for WASM builds)

### TypeScript Build

```bash
npm install
npm run build
npm test
```

### WASM Build

```bash
cd cpp
./scripts/setup-emsdk.sh   # First time only
./scripts/build-wasm.sh
```

Output: `wasm/flatsql.js` and `wasm/flatsql.wasm`

### Native C++ Build

```bash
cd cpp
./scripts/build-native.sh
./build/flatsql_test
```

### Run Local Demo

```bash
npm run serve
# Open http://localhost:8080
```

## SQL Support

### Supported

- `SELECT` with column selection
- `WHERE` with `=`, `<`, `>`, `<=`, `>=`, `BETWEEN`, `AND`, `OR`
- `ORDER BY` (ASC/DESC)
- `LIMIT` and `OFFSET`
- Index-accelerated queries on `(key)` and `(indexed)` columns

### Not Supported

- `JOIN` (query one table at a time)
- `GROUP BY`, `HAVING`, aggregates (`COUNT`, `SUM`, etc.)
- `INSERT`, `UPDATE`, `DELETE` (use API methods instead)
- Subqueries, CTEs, window functions
- `DISTINCT`, `UNION` (except via multi-source views)

## Limitations

| Limitation | Reason |
|------------|--------|
| **Append-only storage** | Designed for streaming/immutable data. No UPDATE/DELETE. |
| **In-memory indexes** | B-trees rebuilt on startup. Not persisted. |
| **No JOINs** | Virtual tables don't support cross-table queries. |
| **Read-only SQL** | Writes go through `insert()` / `ingest()` API. |
| **Single-threaded** | WASM runs on main thread. Use workers for parallelism. |

## Connection to Digital Arsenal FlatBuffers

This project uses [Digital Arsenal's FlatBuffers fork](https://github.com/digitalarsenal/flatbuffers), which provides:

- **[flatc-wasm](https://digitalarsenal.github.io/flatbuffers/)** — The FlatBuffer compiler running in WebAssembly
- **Browser-native compilation** — Generate FlatBuffer code without native tooling
- **Schema validation** — Parse and validate `.fbs` schemas in the browser

The `flatc-wasm` dependency enables FlatSQL to:
1. Parse FlatBuffer schemas at runtime
2. Build FlatBuffers from JavaScript objects
3. Extract fields from FlatBuffer binary data

```javascript
import { FlatcRunner } from 'flatc-wasm';

const flatc = await FlatcRunner.init();
const accessor = new FlatcAccessor(flatc, schema);
// Now FlatSQL can read/write FlatBuffers
```

## API Reference

### FlatSQLDatabase

```typescript
class FlatSQLDatabase {
  // Create from schema
  static fromSchema(schema: string, accessor: FlatBufferAccessor, name: string): FlatSQLDatabase;

  // Table operations
  listTables(): string[];
  getTableDef(name: string): TableDefinition;

  // Data operations
  insert(table: string, record: Record<string, any>): number;
  insertRaw(table: string, flatbuffer: Uint8Array): number;
  stream(table: string, flatbuffers: Uint8Array[]): number[];

  // Query
  query(sql: string): QueryResult;

  // Export
  exportData(): Uint8Array;
  getStats(): TableStats[];
}
```

### QueryResult

```typescript
interface QueryResult {
  columns: string[];
  rows: any[][];
  rowCount: number;
}
```

## Performance

### Benchmark Results

FlatSQL outperforms SQLite on all speed metrics. The following benchmarks compare FlatSQL's native C++ implementation against SQLite for 10,000 records with 10,000 query iterations.

**Test Environment:**

- Apple M3 Ultra (28 cores: 20 performance, 8 efficiency)
- 256 GB RAM
- macOS 15.6
- Native C++ build (not WASM)

#### Ingest Performance

| Operation | FlatSQL | SQLite | Result |
|-----------|---------|--------|--------|
| Indexed ingest (10k records) | 3.87 ms | 4.56 ms | **FlatSQL 1.2x faster** |
| Throughput | 2.58M rec/sec | 2.19M rec/sec | |

#### Query Performance

| Operation | FlatSQL | SQLite | Result |
|-----------|---------|--------|--------|
| Point query (by id) | 3.50 ms | 3.93 ms | **FlatSQL 1.1x faster** |
| Point query (by email) | 5.23 ms | 6.94 ms | **FlatSQL 1.3x faster** |
| Direct index lookup | 1.56 ms | 3.93 ms | **FlatSQL 2.5x faster** |
| Direct single lookup | 0.92 ms | 3.93 ms | **FlatSQL 4.3x faster** |
| Zero-copy lookup | 1.12 ms | 3.93 ms | **FlatSQL 3.5x faster** |
| Full scan (with results) | 0.84 ms | 1.25 ms | **FlatSQL 1.5x faster** |
| Direct iteration | 0.05 ms | 1.25 ms | **FlatSQL 25x faster** |

#### Storage

| Metric | FlatSQL | SQLite | Notes |
|--------|---------|--------|-------|
| Size (10k records) | 0.83 MB | 0.68 MB | FlatSQL stores full FlatBuffers |

FlatSQL uses more storage because it preserves complete FlatBuffer data with vtables and alignment padding. This is an intentional design trade-off that enables zero-copy access and memory-mapped file support.

### WASM Performance

The WebAssembly build runs in browsers and Node.js with similar relative performance characteristics. WASM adds approximately 2-3x overhead compared to native, but maintains the same performance advantages over equivalent JavaScript implementations.

## License

MIT

## Contributing

Contributions welcome. Please open an issue first to discuss significant changes.

## Contact

For questions, licensing inquiries, or commercial support: [tj@digitalarsenal.io](mailto:tj@digitalarsenal.io)

---

Built with [FlatBuffers](https://github.com/digitalarsenal/flatbuffers) and [SQLite](https://sqlite.org/).
