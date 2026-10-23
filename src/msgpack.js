const te = new TextEncoder();
const td = new TextDecoder('utf-8', { fatal: true });

export function encode(value) {
  const chunks = [];
  let size = 0;
  const push = (buf) => { chunks.push(buf); size += buf.length; };
  const one = (byte) => push(Uint8Array.of(byte));
  const num = (bytes, setter, value) => {
    const b = new Uint8Array(bytes); const v = new DataView(b.buffer); setter(v, value); push(b);
  };
  const write = (v) => {
    if (v === null || v === undefined) return one(0xc0);
    if (v === false) return one(0xc2);
    if (v === true) return one(0xc3);
    if (typeof v === 'number') {
      if (Number.isInteger(v) && Number.isSafeInteger(v)) {
        if (v >= 0) {
          if (v <= 0x7f) return one(v);
          if (v <= 0xff) { one(0xcc); return one(v); }
          if (v <= 0xffff) { one(0xcd); return num(2, (d,x)=>d.setUint16(0,x), v); }
          if (v <= 0xffffffff) { one(0xce); return num(4, (d,x)=>d.setUint32(0,x), v); }
          one(0xcf); return num(8, (d,x)=>d.setBigUint64(0,BigInt(x)), v);
        }
        if (v >= -32) return one(0x100 + v);
        if (v >= -128) { one(0xd0); return num(1, (d,x)=>d.setInt8(0,x), v); }
        if (v >= -32768) { one(0xd1); return num(2, (d,x)=>d.setInt16(0,x), v); }
        if (v >= -2147483648) { one(0xd2); return num(4, (d,x)=>d.setInt32(0,x), v); }
        one(0xd3); return num(8, (d,x)=>d.setBigInt64(0,BigInt(x)), v);
      }
      one(0xcb); return num(8, (d,x)=>d.setFloat64(0,x), v);
    }
    if (typeof v === 'bigint') {
      if (v >= 0n) { one(0xcf); return num(8, (d,x)=>d.setBigUint64(0,x), v); }
      one(0xd3); return num(8, (d,x)=>d.setBigInt64(0,x), v);
    }
    if (typeof v === 'string') {
      const b = te.encode(v), n=b.length;
      if (n <= 31) one(0xa0 | n);
      else if (n <= 0xff) { one(0xd9); one(n); }
      else if (n <= 0xffff) { one(0xda); num(2,(d,x)=>d.setUint16(0,x),n); }
      else { one(0xdb); num(4,(d,x)=>d.setUint32(0,x),n); }
      return push(b);
    }
    if (Buffer.isBuffer(v) || v instanceof Uint8Array) {
      const b = new Uint8Array(v), n=b.length;
      if (n <= 0xff) { one(0xc4); one(n); }
      else if (n <= 0xffff) { one(0xc5); num(2,(d,x)=>d.setUint16(0,x),n); }
      else { one(0xc6); num(4,(d,x)=>d.setUint32(0,x),n); }
      return push(b);
    }
    if (Array.isArray(v)) {
      const n=v.length;
      if (n <= 15) one(0x90 | n);
      else if (n <= 0xffff) { one(0xdc); num(2,(d,x)=>d.setUint16(0,x),n); }
      else { one(0xdd); num(4,(d,x)=>d.setUint32(0,x),n); }
      for (const item of v) write(item); return;
    }
    if (typeof v === 'object') {
      const entries = Object.entries(v).filter(([,x]) => x !== undefined), n=entries.length;
      if (n <= 15) one(0x80 | n);
      else if (n <= 0xffff) { one(0xde); num(2,(d,x)=>d.setUint16(0,x),n); }
      else { one(0xdf); num(4,(d,x)=>d.setUint32(0,x),n); }
      for (const [k,x] of entries) { write(k); write(x); } return;
    }
    throw new TypeError(`unsupported MessagePack value: ${typeof v}`);
  };
  write(value);
  return Buffer.concat(chunks.map(Buffer.from), size);
}

export function decode(input) {
  const b = input instanceof Uint8Array ? input : new Uint8Array(input);
  const view = new DataView(b.buffer, b.byteOffset, b.byteLength);
  let o=0;
  const need=(n)=>{ if(o+n>b.length) throw new Error('truncated MessagePack'); };
  const u8=()=>{need(1); return b[o++];};
  const u16=()=>{need(2); const x=view.getUint16(o);o+=2;return x;};
  const u32=()=>{need(4); const x=view.getUint32(o);o+=4;return x;};
  const i8=()=>{need(1);const x=view.getInt8(o);o++;return x;};
  const i16=()=>{need(2);const x=view.getInt16(o);o+=2;return x;};
  const i32=()=>{need(4);const x=view.getInt32(o);o+=4;return x;};
  const integer64=(signed)=>{need(8);const x=signed?view.getBigInt64(o):view.getBigUint64(o);o+=8;return x<=BigInt(Number.MAX_SAFE_INTEGER)&&x>=BigInt(Number.MIN_SAFE_INTEGER)?Number(x):x;};
  const bytes=(n)=>{need(n);const x=b.subarray(o,o+n);o+=n;return x;};
  // Short ASCII strings (most keys and values) skip TextDecoder, whose call costs more than the copy.
  const str=(n)=>{if(n>32)return td.decode(bytes(n));need(n);let x='';for(let i=o;i<o+n;i++){const c=b[i];if(c>0x7f)return td.decode(bytes(n));x+=String.fromCharCode(c);}o+=n;return x;};
  const read=()=>{
    const c=u8();
    if(c<=0x7f)return c; if(c>=0xe0)return c-256;
    if((c&0xe0)===0xa0)return str(c&0x1f);
    if((c&0xf0)===0x90){const a=[];for(let i=0;i<(c&15);i++)a.push(read());return a;}
    if((c&0xf0)===0x80){const x={};for(let i=0;i<(c&15);i++)x[String(read())]=read();return x;}
    switch(c){
      case 0xc0:return null; case 0xc2:return false; case 0xc3:return true;
      case 0xc4:return Buffer.from(bytes(u8())); case 0xc5:return Buffer.from(bytes(u16())); case 0xc6:return Buffer.from(bytes(u32()));
      case 0xca:{need(4);const x=view.getFloat32(o);o+=4;return x;} case 0xcb:{need(8);const x=view.getFloat64(o);o+=8;return x;}
      case 0xcc:return u8(); case 0xcd:return u16(); case 0xce:return u32(); case 0xcf:return integer64(false);
      case 0xd0:return i8(); case 0xd1:return i16(); case 0xd2:return i32(); case 0xd3:return integer64(true);
      case 0xd9:return str(u8()); case 0xda:return str(u16()); case 0xdb:return str(u32());
      case 0xdc:{const n=u16(),a=[];for(let i=0;i<n;i++)a.push(read());return a;} case 0xdd:{const n=u32(),a=[];for(let i=0;i<n;i++)a.push(read());return a;}
      case 0xde:{const n=u16(),x={};for(let i=0;i<n;i++)x[String(read())]=read();return x;} case 0xdf:{const n=u32(),x={};for(let i=0;i<n;i++)x[String(read())]=read();return x;}
      default: throw new Error(`unsupported MessagePack marker 0x${c.toString(16)}`);
    }
  };
  const result=read(); if(o!==b.length)throw new Error('trailing MessagePack bytes'); return result;
}
