export interface EvoDBOptions {
  host?: string;
  port?: number;
  pool?: number;
}

export interface ColumnInfo {
  name: string;
  type: 'INT' | 'FLOAT' | 'STRING' | 'BOOL' | 'JSON' | 'NULL';
  indexed: boolean;
}

export interface WhereClause {
  col: string;
  op?: '=' | '!=' | '<' | '<=' | '>' | '>=';
  val: string | number | boolean | null;
}

export interface OrderBy {
  col: string;
  dir?: 'ASC' | 'DESC';
}

export interface PullOptions {
  where?: WhereClause[];
  orderBy?: OrderBy;
  limit?: number;
}

export type Schema = Record<string, string>;
export type Row = Record<string, string>;

export class EvoDB {
  constructor(opts?: EvoDBOptions);

  /** Connect to the EvoDB server. */
  connect(): Promise<this>;

  /** Close all connections. */
  close(): void;

  /** Send a raw command string. */
  raw(cmd: string): Promise<unknown>;

  // Table management
  forge(table: string, schema: Schema): Promise<string>;
  drop(table: string): Promise<string>;
  tables(): Promise<string[]>;
  schema(table: string): Promise<ColumnInfo[]>;
  index(table: string, col: string): Promise<string>;

  // Writing
  push(table: string, values: Array<string | number | boolean | null>): Promise<string>;
  upsert(table: string, keyCol: string, values: Array<string | number | boolean | null>): Promise<string>;
  reforge(table: string, where: { col: string; val: any }, set: { col: string; val: any }): Promise<number>;
  burn(table: string, col: string, val: any): Promise<number>;

  // Reading
  pull(table: string, opts?: PullOptions): Promise<Row[]>;
  pullWhere(table: string, col: string, val: any, limit?: number): Promise<Row[]>;
  count(table: string, col?: string, val?: any): Promise<number>;
}
