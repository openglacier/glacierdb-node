import test from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { Client } from '../src/index.js';
import { encode, decode } from '../src/msgpack.js';

function frame(obj){const p=encode(obj),b=Buffer.alloc(4+p.length);b.writeUInt32BE(p.length);p.copy(b,4);return b;}
function mockServer(){
  let connections=0, identityOpens=0, queries=0;
  const server=net.createServer(sock=>{connections++;let rx=Buffer.alloc(0);sock.on('data',chunk=>{rx=Buffer.concat([rx,chunk]);while(rx.length>=4){const n=rx.readUInt32BE(0);if(rx.length<4+n)return;const m=decode(rx.subarray(4,4+n));rx=rx.subarray(4+n);if(m.op==='identity.open'){identityOpens++;sock.write(frame({version:1,kind:'response',id:m.id,status:'ok',data:{authenticated:true}}));}else if(m.op==='query.execute'){queries++;sock.write(frame({version:1,kind:'response',id:m.id,status:'complete',statistics:{inserted:1}}));}}});});
  return {server,stats:()=>({connections,identityOpens,queries})};
}

test('MessagePack roundtrip',()=>{const x={version:1,id:7,op:'query.execute',data:{query:'from café',n:-2,ok:true,items:[1,null,'x']}};assert.deepEqual(decode(encode(x)),x);});

test('multi-batch .ogid load keeps one connection and one identity.open',async()=>{
  const {server,stats}=mockServer();await new Promise(r=>server.listen(0,'127.0.0.1',r));
  const dir=await fs.mkdtemp(path.join(os.tmpdir(),'glacierdb-node-'));const ogid=path.join(dir,'x.ogid');await fs.writeFile(ogid,'encrypted-placeholder');
  const {port}=server.address();const c=new Client({port,identity:ogid,identityPassword:'secret'});
  try{
    const docs=Array.from({length:7},(_,i)=>({i,payload:'x'.repeat(80)}));
    const result=await c.load('items',docs,{chunkSize:2,maxRequestBytes:2048,requestHeadroom:256});
    assert.equal(result.rowsRead,7);assert.ok(stats().queries>=4);assert.equal(stats().connections,1);assert.equal(stats().identityOpens,1);
  } finally {c.close();server.close();await fs.rm(dir,{recursive:true,force:true});}
});

function queryServer(respond){
  const server=net.createServer(sock=>{let rx=Buffer.alloc(0);sock.on('data',chunk=>{rx=Buffer.concat([rx,chunk]);while(rx.length>=4){const n=rx.readUInt32BE(0);if(rx.length<4+n)return;const m=decode(rx.subarray(4,4+n));rx=rx.subarray(4+n);respond(sock,m);}});});
  return new Promise(r=>server.listen(0,'127.0.0.1',()=>r(server)));
}

test('a result of 200k documents builds without overflowing the stack',async()=>{
  const total=200_000;
  const server=await queryServer((sock,m)=>{
    const frames=[];for(let i=0;i<total;i++)frames.push(frame({version:1,kind:'response',id:m.id,status:'partial',data:{i}}));
    frames.push(frame({version:1,kind:'response',id:m.id,status:'complete',statistics:{returned:total}}));
    sock.write(Buffer.concat(frames));
  });
  const c=new Client({port:server.address().port});
  try{const result=await c.execute('on items');assert.equal(result.length,total);assert.equal(result[total-1].i,total-1);assert.equal(result.statistics.returned,total);}
  finally{c.close();server.close();}
});

test('a large frame split into small chunks is reassembled',async()=>{
  const documents=Array.from({length:20_000},(_,i)=>({i,payload:'x'.repeat(100)}));
  const server=await queryServer((sock,m)=>{
    const bytes=frame({version:1,kind:'response',id:m.id,status:'ok',data:{documents,statistics:{}}});
    for(let o=0;o<bytes.length;o+=1000)sock.write(bytes.subarray(o,o+1000));
  });
  const c=new Client({port:server.address().port});
  try{const result=await c.execute('on items');assert.equal(result.length,documents.length);assert.deepEqual(result[19_999],documents[19_999]);}
  finally{c.close();server.close();}
});

test('the timeout bounds the silence between frames, not the whole response',async()=>{
  const server=await queryServer((sock,m)=>{
    let i=0;const tick=setInterval(()=>{
      if(i<6){sock.write(frame({version:1,kind:'response',id:m.id,status:'partial',data:{i:i++}}));return;}
      clearInterval(tick);sock.write(frame({version:1,kind:'response',id:m.id,status:'complete',statistics:{}}));
    },60);
  });
  const c=new Client({port:server.address().port,timeout:200});
  try{assert.equal((await c.execute('on items')).length,6);}finally{c.close();}
  const silent=await queryServer(()=>{});
  const d=new Client({port:silent.address().port,timeout:100});
  try{await assert.rejects(d.execute('on items'),/timed out/);}finally{d.close();server.close();silent.close();}
});
