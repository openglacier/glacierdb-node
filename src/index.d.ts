/// <reference types="node" />
export type Document = Record<string, unknown>;
export type RequestId = number | string;
export interface ClientOptions { host?: string; port?: number; timeout?: number; connectTimeout?: number; identity?: string | Document | null; identityPassword?: string | null; }
export interface LoadProgress { collection:string; batch:number; rowsDone:number; totalRows:number|null; rowsPerSecond:number; elapsedSeconds:number; etaSeconds:number|null; }
export interface LoadOptions { chunkSize?:number; mode?:'merge'|'replace'|'update'; maxRequestBytes?:number; requestHeadroom?:number; progress?:boolean|((progress:LoadProgress)=>void); totalRows?:number|null; }
export interface CsvLoadOptions extends LoadOptions { delimiter?:string; headers?:boolean; transform?:(document:Document)=>Document|Promise<Document>; }
export interface LoadResult { collection:string; rowsRead:number; inserted:number; replaced:number; chunksLoaded:number; requestsSent:number; bytesSent:number; elapsedSeconds:number; rowsPerSecond:number; }
export class GlacierDBError extends Error {}
export class ConnectionError extends GlacierDBError {}
export class ProtocolError extends GlacierDBError {}
export class QueryError extends GlacierDBError { code:string; requestId:RequestId|null; }
export class LoadError extends GlacierDBError { collection:string; rowNumber:number; cause:unknown; }
export class QueryResult extends Array<Document> { statistics:Record<string,unknown>; requestId:RequestId; readonly documents:Document[]; }
export class Client {
  constructor(options?:ClientOptions); readonly connected:boolean; authenticated:boolean;
  connect():Promise<this>; close():void; execute(query:string):Promise<QueryResult>; query(query:string):Promise<QueryResult>;
  insert(collection:string,document:Document):Promise<QueryResult>; operation(op:string,data?:Document):Promise<Document>;
  identityOpen(path:string,password:string):Promise<this>; identityGet(password:string,destination?:string|null):Promise<Buffer|string>; identityRenew(password:string,destination?:string|null):Promise<Buffer|string>;
  load(collection:string,data:Iterable<Document>|AsyncIterable<Document>|string,options?:LoadOptions):Promise<LoadResult>;
  loadCsv(collection:string,path:string,options?:CsvLoadOptions):Promise<LoadResult>;
}
export function connect(address?:string,options?:Omit<ClientOptions,'host'|'port'>):Promise<Client>;
export const PROTOCOL_VERSION:number, PROTOCOL_MAX_REQUEST_BYTES:number, MAX_REQUEST_BYTES:number, MAX_RESPONSE_MESSAGE_BYTES:number;
