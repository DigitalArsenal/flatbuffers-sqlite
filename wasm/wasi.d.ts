export interface FlatSQLWASILoadOptions {
  url?: string | URL;
  path?: string;
  as?: 'arrayBuffer' | 'uint8array';
}

export declare function getFlatSQLWASIURL(): URL;

export declare function loadFlatSQLWASI(
  options?: FlatSQLWASILoadOptions
): Promise<ArrayBuffer | Uint8Array>;
