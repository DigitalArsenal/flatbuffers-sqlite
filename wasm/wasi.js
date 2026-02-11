const DEFAULT_WASI_URL = new URL('./flatsql-wasi.wasm', import.meta.url);

/**
 * Resolve the packaged FlatSQL WASI module URL.
 */
export function getFlatSQLWASIURL() {
  return DEFAULT_WASI_URL;
}

async function readViaFsPath(path) {
  const fs = await import('node:fs/promises');
  return new Uint8Array(await fs.readFile(path));
}

/**
 * Load the packaged FlatSQL WASI module bytes.
 *
 * @param {{
 *   url?: string | URL,
 *   path?: string,
 *   as?: 'arrayBuffer' | 'uint8array'
 * }} [options]
 * @returns {Promise<ArrayBuffer | Uint8Array>}
 */
export async function loadFlatSQLWASI(options = {}) {
  const as = options.as === 'uint8array' ? 'uint8array' : 'arrayBuffer';

  let bytes;
  if (options.path) {
    bytes = await readViaFsPath(options.path);
  } else {
    const wasmUrl = options.url ? new URL(options.url, import.meta.url) : DEFAULT_WASI_URL;

    if (wasmUrl.protocol === 'file:') {
      const { fileURLToPath } = await import('node:url');
      bytes = await readViaFsPath(fileURLToPath(wasmUrl));
    } else {
      const response = await fetch(wasmUrl);
      if (!response.ok) {
        throw new Error(`Failed to load flatsql-wasi.wasm: ${response.status}`);
      }
      bytes = new Uint8Array(await response.arrayBuffer());
    }
  }

  if (as === 'uint8array') {
    return bytes;
  }

  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
}
