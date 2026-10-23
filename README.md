# glacierdb Node.js driver

Zero-dependency Node.js 20+ driver for `ogd`. It uses one persistent TCP connection, the OG MessagePack framing protocol, streaming query responses, core-managed encrypted `.ogid` identities, and byte-bounded bulk loading.

```js
import { connect } from 'glacierdb';

const db = await connect('127.0.0.1:7878', {
  identity: '/home/me/my.ogid',
  identityPassword: process.env.OGID_PASSWORD,
});

const rows = await db.query('from users | limit 10');
console.log(rows.documents, rows.statistics);

await db.load('events', documents, {
  chunkSize: 10_000,
  progress: true,
});

db.close();
```

## CSV import

```js
await db.loadCsv('events', './events.csv', {
  chunkSize: 100_000,
  progress: true,
  transform(row) {
    row.value = Number(row.value);
    return row;
  },
});
```

`chunkSize` is a row ceiling. Like the Python driver, the Node driver flushes earlier when the encoded request approaches `MAX_REQUEST_BYTES` (256 KiB by default).

### Connection behavior

Bulk loads **never reconnect between batches**. An encrypted `.ogid` is opened once when the TCP connection is established. If the socket is genuinely lost, the next operation may reconnect and authenticate again. This avoids the Python regression where expensive `identity.open` calls were accidentally performed for every load batch.

## Identity operations

```js
await db.identityOpen('./identity.ogid', password);
await db.identityGet('new-password', './copy.ogid');
await db.identityRenew('new-password', './renewed.ogid');
```

Legacy JSON identities containing `identityId`, `deviceId` and a PKCS#8 Ed25519 `privateKey` are also supported.

## API

- `connect(address, options)` / `new Client(options)`
- `execute(query)` / `query(query)`
- `insert(collection, document)`
- `load(collection, iterable, options)` — accepts sync or async iterables
- `loadCsv(collection, path, options)` — streaming CSV parser, no full-file buffering
- `operation(op, data)`
- `identityOpen`, `identityGet`, `identityRenew`
- `close()`

The package has no runtime npm dependencies; its MessagePack codec and CSV streaming parser are included.
