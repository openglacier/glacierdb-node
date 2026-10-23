import { connect } from '../src/index.js';
const db = await connect(process.env.OG_ADDRESS ?? '127.0.0.1:7878', {
  identity: process.env.OG_IDENTITY ?? null,
  identityPassword: process.env.OG_IDENTITY_PASSWORD ?? null,
});
try {
  const result = await db.query('from users | limit 10');
  console.log(result.documents);
} finally { db.close(); }
