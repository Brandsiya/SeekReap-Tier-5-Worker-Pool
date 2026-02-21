// ==========================
// start.js
// ==========================

const { Client } = require('pg');
const express = require('express');

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  console.error('DATABASE_URL not set');
  process.exit(1);
}

// PostgreSQL client
const client = new Client({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// Express server
const app = express();
app.use(express.json());

// ---------------- Worker ----------------
async function processJob(job) {
  console.log(`Processing job ${job.job_id}: ${job.content_id}`);
  try {
    await new Promise(r => setTimeout(r, 2000));
    await client.query("UPDATE job_queue SET status=$1, completed_at=NOW() WHERE job_id=$2", ["completed", job.job_id]);
    const contentUrl = `https://cdn.example.com/${job.content_id}.mp4`;
    await client.query("INSERT INTO content_results (job_id, creator_id, job_type, result_url) VALUES ($1,$2,$3,$4)", [job.job_id, job.creator_id, job.job_type, contentUrl]);
    console.log(`Job ${job.job_id} completed and result inserted`);
  } catch (err) {
    console.error(`Job ${job.job_id} failed`, err);
    await client.query("UPDATE job_queue SET status=$1 WHERE job_id=$2", ["failed", job.job_id]);
  }
}

// Run worker asynchronously
async function runWorker() {
  await client.connect();
  console.log('Worker connected to DB');

  while (true) {
    const res = await client.query(
      "SELECT job_id, creator_id, content_id, job_type FROM job_queue WHERE status='pending' ORDER BY job_id ASC LIMIT 1"
    );

    if (res.rows.length === 0) {
      await new Promise(r => setTimeout(r, 2000));
      continue;
    }

    const job = res.rows[0];
    await client.query(
      "UPDATE job_queue SET status='in_progress', started_at=NOW() WHERE job_id=$1",
      [job.job_id]
    );

    await processJob(job);
  }
}

// Run worker asynchronously
runWorker().catch(err => { console.error('Worker error', err); process.exit(1); });

// ---------------- Server ----------------
app.get('/creator/:id/jobs', async (req, res) => {
  try {
    const results = await client.query(
      'SELECT * FROM content_results WHERE creator_id = $1 ORDER BY created_at DESC',
      [req.params.id]
    );
    res.json(results.rows);
  } catch (err) {
    console.error('Error fetching jobs', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
