import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';

const app = express();
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, '..'); // project root

app.use(express.static(root));
app.get('/api/ping', (req,res)=>res.json({ok:true}));

const port = process.env.PORT || 8787;
app.listen(port, () => {
  console.log(`Static server at http://localhost:${port}`);
});
