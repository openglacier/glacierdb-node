import net from 'node:net';
import fs from 'node:fs';
import { createPrivateKey, sign as cryptoSign } from 'node:crypto';
import { encode, decode } from './msgpack.js';

export const PROTOCOL_VERSION = 1;
export const PROTOCOL_MAX_REQUEST_BYTES = 8 * 1024 * 1024;
export const MAX_REQUEST_BYTES = 256 * 1024;
export const MAX_RESPONSE_MESSAGE_BYTES = 8 * 1024 * 1024;
const DEFAULT_REQUEST_HEADROOM = 8192;
const COLLECTION_RE = /^[A-Za-z_][A-Za-z0-9_.-]*$/;
const LOAD_MODES = new Set(['merge','replace','update']);

export class GlacierDBError extends Error { constructor(message){super(message);this.name=this.constructor.name;} }
export class ConnectionError extends GlacierDBError {}
export class ProtocolError extends GlacierDBError {}
export class QueryError extends GlacierDBError {
  constructor(code,message,requestId=null){super(`${requestId!=null?`request ${requestId}: `:''}${code}: ${message}`);this.code=code;this.requestId=requestId;}
}
export class LoadError extends GlacierDBError {
  constructor(message,{collection,rowNumber,cause}){super(`${message} (collection=${JSON.stringify(collection)}, row=${rowNumber}): ${cause?.message ?? cause}`);this.collection=collection;this.rowNumber=rowNumber;this.cause=cause;}
}

export class QueryResult extends Array {
  static get [Symbol.species](){ return Array; }
  constructor(documents=[], statistics={}, requestId=0){super();for(const document of documents)this.push(document);this.statistics=statistics;this.requestId=requestId;}
  get documents(){ return Array.from(this); }
}

export class Client {
  constructor({host='127.0.0.1',port=7878,timeout=30000,connectTimeout=5000,identity=null,identityPassword=null}={}){
    this.host=host;this.port=port;this.timeout=timeout;this.connectTimeout=connectTimeout;this.identity=identity;this.identityPassword=identityPassword;
    this.socket=null;this.authenticated=false;this.nextRequestId=1;this.pending=new Map();this.rxChunks=[];this.rxBytes=0;this.rxNeed=4;this.connectPromise=null;
  }
  get connected(){return !!this.socket && !this.socket.destroyed;}
  async connect(){
    if(this.connected)return this;
    if(this.connectPromise)return this.connectPromise;
    this.connectPromise=(async()=>{
      const socket=new net.Socket(); socket.setNoDelay(true); socket.setKeepAlive(true);
      await new Promise((resolve,reject)=>{
        const timer=setTimeout(()=>{socket.destroy();reject(new ConnectionError(`cannot connect to ogd at ${this.host}:${this.port}: timeout`));},this.connectTimeout);
        socket.once('error',onError); socket.connect(this.port,this.host,()=>{clearTimeout(timer);socket.off('error',onError);resolve();});
        function onError(e){clearTimeout(timer);reject(new ConnectionError(`cannot connect to ogd: ${e.message}`));}
      });
      this.socket=socket;this.authenticated=false;this.rxChunks=[];this.rxBytes=0;this.rxNeed=4;
      socket.on('data',(chunk)=>this.#onData(chunk));
      socket.on('error',(e)=>this.#disconnect(new ConnectionError(`communication with ogd failed: ${e.message}`)));
      socket.on('close',()=>this.#disconnect(new ConnectionError('ogd closed the connection')));
      if(this.identity){
        if(isOgid(this.identity)){
          if(this.identityPassword==null)throw new TypeError('identityPassword is required for .ogid authentication');
          await this.identityOpen(this.identity,this.identityPassword);
        } else await this.#authenticateLegacy(this.identity);
      }
      return this;
    })();
    try{return await this.connectPromise;}catch(e){this.close();throw e;}finally{this.connectPromise=null;}
  }
  close(){ const s=this.socket; this.socket=null; this.authenticated=false; if(s && !s.destroyed)s.destroy(); }

  async execute(query){
    if(typeof query!=='string'||!query.trim())throw new TypeError('query must be a non-empty string');
    await this.connect();
    const id=this.#nextId();
    return this.#request({version:1,id,op:'query.execute',data:{query}}, {type:'query',maximumBytes:MAX_REQUEST_BYTES});
  }
  query(query){return this.execute(query);}
  async insert(collection, document){ return this.execute(`from ${validateCollection(collection)} | insert ${JSON.stringify(normalizeDocument(document))}`); }

  async operation(op,data={}){ await this.connect(); const id=this.#nextId(); return this.#request({version:1,id,op,data},{type:'operation',maximumBytes:PROTOCOL_MAX_REQUEST_BYTES}); }
  async identityOpen(path,password){
    await this.connect();
    const raw=await fs.promises.readFile(path);
    const out=await this.operation('identity.open',{file:raw.toString('base64'),password});
    if(out.authenticated!==true)throw new ProtocolError('identity.open did not confirm authentication');
    this.authenticated=true;return this;
  }
  async identityGet(password,destination=null){return this.#identityFileResult(await this.operation('identity.get',{password}),destination);}
  async identityRenew(password,destination=null){const out=await this.operation('identity.renew',{password});this.authenticated=true;return this.#identityFileResult(out,destination);}

  async load(collection,data,{chunkSize=10000,mode='merge',maxRequestBytes=MAX_REQUEST_BYTES,requestHeadroom=DEFAULT_REQUEST_HEADROOM,progress=false,totalRows=null}={}){
    collection=validateCollection(collection); mode=validateMode(mode);
    if(!Number.isInteger(chunkSize)||chunkSize<=0)throw new RangeError('chunkSize must be > 0');
    if(maxRequestBytes<1024||maxRequestBytes>PROTOCOL_MAX_REQUEST_BYTES)throw new RangeError(`maxRequestBytes must be between 1024 and ${PROTOCOL_MAX_REQUEST_BYTES}`);
    if(requestHeadroom<0||requestHeadroom>=maxRequestBytes)throw new RangeError('requestHeadroom must be in [0, maxRequestBytes)');
    if(typeof data==='string' && (data.endsWith('.csv')||fs.existsSync(data))) return this.loadCsv(collection,data,{chunkSize,mode,maxRequestBytes,requestHeadroom,progress});
    const started=performance.now(); let rowsRead=0,inserted=0,replaced=0,requestsSent=0,bytesSent=0;
    let encodedDocs=[], escapedBytes=0, firstRow=1;
    const payloadBudget=maxRequestBytes-requestHeadroom;
    const emptySize=this.#requestSize(buildLoadQuery(collection,mode,'[]'));
    const documentBudget=payloadBudget-emptySize;
    const iterable=toAsyncIterable(data);
    const flush=async()=>{
      if(!encodedDocs.length)return;
      const query=buildLoadQuery(collection,mode,`[${encodedDocs.join(',')}]`);
      const payloadSize=this.#requestSize(query);
      try{
        const result=await this.#executeWithLimit(query,maxRequestBytes);
        inserted+=Number(result.statistics?.inserted ?? encodedDocs.length); replaced+=Number(result.statistics?.replaced ?? 0);
      }catch(cause){throw new LoadError('cannot load batch',{collection,rowNumber:firstRow,cause});}
      requestsSent++;bytesSent+=payloadSize;
      if(progress){
        const elapsed=(performance.now()-started)/1000;
        const update={collection,batch:requestsSent,rowsDone:rowsRead,totalRows,rowsPerSecond:elapsed?rowsRead/elapsed:0,elapsedSeconds:elapsed,etaSeconds:totalRows&&rowsRead?Math.max(totalRows-rowsRead,0)/(rowsRead/elapsed):null};
        if(typeof progress==='function')progress(update); else process.stderr.write(`\r${collection}: ${rowsRead}${totalRows?`/${totalRows}`:''} rows | ${Math.round(update.rowsPerSecond).toLocaleString()} rows/s`);
      }
      encodedDocs=[];escapedBytes=0;firstRow=rowsRead+1;
    };
    for await(const document of iterable){
      const encoded=JSON.stringify(normalizeDocument(document));
      const encodedSize=escapedQueryFragmentSize(encoded), sep=encodedDocs.length?1:0;
      if(encodedSize>documentBudget)throw new LoadError('one document exceeds the configured request size',{collection,rowNumber:rowsRead+1,cause:new ProtocolError(`escaped document is ${encodedSize} bytes; budget is ${documentBudget}`)});
      if(encodedDocs.length && (encodedDocs.length>=chunkSize || escapedBytes+sep+encodedSize>documentBudget)) await flush();
      encodedDocs.push(encoded);escapedBytes+= (encodedDocs.length>1?1:0)+encodedSize;rowsRead++;
    }
    await flush();
    const elapsedSeconds=(performance.now()-started)/1000;
    if(progress===true)process.stderr.write('\n');
    return {collection,rowsRead,inserted,replaced,chunksLoaded:requestsSent,requestsSent,bytesSent,elapsedSeconds,rowsPerSecond:elapsedSeconds?rowsRead/elapsedSeconds:0};
  }

  async loadCsv(collection,path,options={}){
    const {delimiter=',',headers=true,transform=null,...loadOptions}=options;
    const rows=csvRows(fs.createReadStream(path,{encoding:'utf8'}),delimiter);
    let header=null;
    const docs=(async function*(){
      for await(const row of rows){
        if(header===null && headers){header=row;continue;}
        const keys=header ?? row.map((_,i)=>String(i));
        if(row.length!==keys.length)throw new Error(`CSV row has ${row.length} fields; expected ${keys.length}`);
        let doc=Object.fromEntries(keys.map((k,i)=>[k,row[i]]));
        if(transform)doc=await transform(doc);
        yield doc;
      }
    })();
    return this.load(collection,docs,loadOptions);
  }

  async #executeWithLimit(query,maximumBytes){await this.connect();const id=this.#nextId();return this.#request({version:1,id,op:'query.execute',data:{query}},{type:'query',maximumBytes});}
  #requestSize(query){return encode({version:1,id:this.nextRequestId,op:'query.execute',data:{query}}).length;}
  #nextId(){return this.nextRequestId++;}
  async #request(message,{type,maximumBytes}){
    const payload=encode(message); if(payload.length>maximumBytes)throw new ProtocolError(`request is ${payload.length} bytes; maximum is ${maximumBytes}`);
    if(!this.connected)throw new ConnectionError('not connected');
    const id=message.id;
    const key=String(id);
    const result=new Promise((resolve,reject)=>{
      const p={id,type,documents:[],timer:null};
      // The timeout bounds the silence between two frames, not the whole response.
      // A frame only stamps the time; the timer rearms itself when it fires early.
      p.touch=()=>{p.last=performance.now();};
      const expire=()=>{const idle=performance.now()-p.last;if(idle<this.timeout){p.timer=setTimeout(expire,this.timeout-idle);return;}if(this.pending.get(key)===p){this.pending.delete(key);reject(new ConnectionError(`request ${id} timed out`));}};
      p.resolve=(x)=>{clearTimeout(p.timer);resolve(x);};p.reject=(e)=>{clearTimeout(p.timer);reject(e);};
      this.pending.set(key,p);p.touch();if(this.timeout>0)p.timer=setTimeout(expire,this.timeout);
    });
    const frame=Buffer.allocUnsafe(4+payload.length);frame.writeUInt32BE(payload.length,0);payload.copy(frame,4);
    await new Promise((resolve,reject)=>this.socket.write(frame,e=>e?reject(e):resolve()));
    return result;
  }
  #onData(chunk){
    // Chunks wait in a list until the next frame is whole, then are joined once.
    this.rxChunks.push(chunk);this.rxBytes+=chunk.length;
    if(this.rxBytes<this.rxNeed)return;
    let rx=this.rxChunks.length===1?this.rxChunks[0]:Buffer.concat(this.rxChunks,this.rxBytes);
    this.rxChunks=[];
    while(rx.length>=4){const n=rx.readUInt32BE(0);if(n<1||n>MAX_RESPONSE_MESSAGE_BYTES){this.#disconnect(new ProtocolError(`response payload length ${n} is outside the protocol limit`));return;}if(rx.length<4+n)break;
      const payload=rx.subarray(4,4+n);rx=rx.subarray(4+n);
      let msg;try{msg=decode(payload);}catch(e){this.#disconnect(new ProtocolError(`invalid MessagePack response: ${e.message}`));return;}
      this.#handleMessage(msg);
    }
    if(rx.length)this.rxChunks.push(rx);
    this.rxBytes=rx.length;this.rxNeed=rx.length>=4?4+rx.readUInt32BE(0):4;
  }
  #handleMessage(msg){
    if(!msg||typeof msg!=='object'||msg.version!==PROTOCOL_VERSION){this.#disconnect(new ProtocolError('invalid protocol message/version'));return;}
    if(msg.kind==='event')return;
    const p=this.pending.get(String(msg.id)); if(!p)return this.#disconnect(new ProtocolError(`response id mismatch/unexpected id ${msg.id}`));
    p.touch();
    if(msg.status==='error'){this.pending.delete(String(msg.id));const err=remoteError(msg,p.id);p.reject(err);return;}
    if(p.type==='operation'){
      if(msg.status!=='ok'){this.pending.delete(String(msg.id));p.reject(new ProtocolError(`unexpected operation status ${msg.status}`));return;}
      this.pending.delete(String(msg.id));p.resolve(msg.data && typeof msg.data==='object'?msg.data:{});return;
    }
    if(msg.status==='partial'){if(!msg.data||typeof msg.data!=='object'||Array.isArray(msg.data)){this.pending.delete(String(msg.id));p.reject(new ProtocolError('partial query response data must be an object'));}else p.documents.push(msg.data);return;}
    if(msg.status==='complete'){this.pending.delete(String(msg.id));p.resolve(new QueryResult(p.documents,msg.statistics??{},p.id));return;}
    if(msg.status==='ok'){const d=msg.data??{};this.pending.delete(String(msg.id));p.resolve(new QueryResult(Array.isArray(d.documents)?d.documents:[],d.statistics??{},p.id));return;}
    this.pending.delete(String(msg.id));p.reject(new ProtocolError(`unknown query response status ${msg.status}`));
  }
  #disconnect(error){
    const s=this.socket;this.socket=null;this.authenticated=false;if(s&&!s.destroyed)s.destroy();
    for(const p of this.pending.values())p.reject(error);this.pending.clear();
  }
  async #authenticateLegacy(identity){
    let value=identity;
    if(typeof identity==='string')value=JSON.parse(await fs.promises.readFile(identity,'utf8'));
    const begin=await this.operation('auth.begin',{identityId:required(value,'identityId'),deviceId:required(value,'deviceId')});
    const challengeId=required(begin,'challengeId'), challenge=Buffer.from(required(begin,'challenge'),'base64');
    const der=Buffer.from(required(value,'privateKey'),'base64'); const key=createPrivateKey({key:der,format:'der',type:'pkcs8'});
    const signature=cryptoSign(null,challenge,key).toString('base64');
    const done=await this.operation('auth.complete',{challengeId,signature}); if(done.authenticated!==true)throw new ProtocolError('auth.complete did not confirm authentication'); this.authenticated=true;
  }
  async #identityFileResult(data,destination){const encoded=data.file;if(typeof encoded!=='string'||!encoded)throw new ProtocolError('identity operation returned no encrypted file');const raw=Buffer.from(encoded,'base64');if(destination==null)return raw;let target=destination;if((await fs.promises.stat(destination).catch(()=>null))?.isDirectory())target=`${destination}/${data.fileName||'identity.ogid'}`;await fs.promises.writeFile(target,raw);return target;}
}

export async function connect(address='127.0.0.1:7878',options={}){const i=address.lastIndexOf(':');if(i<1)throw new TypeError("address must use the form 'host:port'");const client=new Client({...options,host:address.slice(0,i),port:Number(address.slice(i+1))});return client.connect();}

function isOgid(x){return typeof x==='string'&&x.toLowerCase().endsWith('.ogid');}
function required(x,k){const v=x?.[k];if(typeof v!=='string'||!v)throw new ProtocolError(`field ${k} must be a non-empty string`);return v;}
function validateCollection(x){if(typeof x!=='string'||!COLLECTION_RE.test(x))throw new TypeError('collection must match [A-Za-z_][A-Za-z0-9_.-]*');return x;}
function validateMode(x){if(!LOAD_MODES.has(x))throw new TypeError('mode must be one of: merge, replace, update');return x;}
function buildLoadQuery(c,m,chunk){return `on ${c}\n| load\n    | with ${m}\n    | chunk ${chunk}\n| end`;}
function escapedQueryFragmentSize(s){return Buffer.byteLength(JSON.stringify(s).slice(1,-1),'utf8');}
function normalizeDocument(doc){if(!doc||typeof doc!=='object'||Array.isArray(doc))throw new TypeError('document must be an object');return Object.fromEntries(Object.entries(doc).map(([k,v])=>[String(k),normalizeValue(v)]));}
function normalizeValue(v){if(v==null||typeof v==='boolean'||typeof v==='string')return v;if(typeof v==='number')return Number.isFinite(v)?v:null;if(typeof v==='bigint'){const n=Number(v);if(!Number.isSafeInteger(n))throw new TypeError('BigInt outside JavaScript safe integer range');return n;}if(v instanceof Date)return v.toISOString();if(Buffer.isBuffer(v)||v instanceof Uint8Array)return Buffer.from(v).toString('base64');if(Array.isArray(v))return v.map(normalizeValue);if(typeof v==='object')return Object.fromEntries(Object.entries(v).map(([k,x])=>[String(k),normalizeValue(x)]));throw new TypeError(`unsupported value type: ${typeof v}`);}
async function* toAsyncIterable(data){if(data?.[Symbol.asyncIterator]){yield* data;return;}if(data?.[Symbol.iterator]){yield* data;return;}throw new TypeError('load data must be iterable or async iterable');}
function remoteError(m,id){const e=m.error??m.data??{};return new QueryError(String(e.code??m.code??'query.error'),String(e.message??m.message??'query failed'),id);}

async function* csvRows(stream,delimiter=','){
  if(typeof delimiter!=='string'||delimiter.length!==1)throw new TypeError('delimiter must be one character');
  let row=[],field='',quoted=false,pendingQuote=false;
  for await(const chunk of stream){for(const ch of chunk){
    if(quoted){if(pendingQuote){if(ch==='"'){field+='"';pendingQuote=false;continue;}quoted=false;pendingQuote=false;if(ch===delimiter){row.push(field);field='';continue;}if(ch==='\n'){row.push(field);yield row;row=[];field='';continue;}if(ch==='\r')continue;throw new Error('invalid character after closing CSV quote');}if(ch==='"'){pendingQuote=true;}else field+=ch;continue;}
    if(ch==='"'&&field===''){quoted=true;continue;} if(ch===delimiter){row.push(field);field='';continue;} if(ch==='\n'){row.push(field);yield row;row=[];field='';continue;} if(ch==='\r')continue; field+=ch;
  }}
  if(quoted&&!pendingQuote)throw new Error('unterminated CSV quoted field');
  if(pendingQuote)quoted=false;
  if(field.length||row.length){row.push(field);yield row;}
}
