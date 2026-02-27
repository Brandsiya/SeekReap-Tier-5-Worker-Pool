const { Client } = require('pg');
const express = require('express');
const axios = require('axios');
const PgBoss = require('pg-boss');

// =====================================================
// Configuration
// =====================================================
const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  console.error('❌ DATABASE_URL not set');
  process.exit(1);
}

// PostgreSQL client for direct queries
const pgClient = new Client({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  connectionTimeoutMillis: 10000
});

pgClient.on('error', (err) => {
  console.error('❌ PostgreSQL client error:', err.message);
});

// Express server
const app = express();
app.use(express.json());

const TIER4_URL = process.env.TIER4_URL || 'http://localhost:10000';
console.log(`📡 Tier-4 URL configured: ${TIER4_URL}`);

// =====================================================
// PostgreSQL Queue Setup with pg-boss
// =====================================================
const boss = new PgBoss({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  schema: 'pgboss',  // Creates its own schema
  maxRetries: 5,     // BullMQ compatibility
  retryBackoff: true, // Exponential backoff
  expireInSeconds: 3600,
  archiveCompletedAfterSeconds: 86400  // Archive completed jobs after 1 day
});

boss.on('error', (err) => {
  console.error('❌ pg-boss error:', err.message);
});

boss.on('ready', () => {
  console.log('✅ pg-boss queue system ready');
});

boss.on('wip', (data) => {
  console.log(`🔄 Worker in progress: ${data.name}`);
});

// =====================================================
// Initialize Database and Queue
// =====================================================
async function initDatabase() {
  let connected = false;
  let retries = 5;

  while (!connected && retries > 0) {
    try {
      await pgClient.connect();
      connected = true;
      console.log(`✅ PostgreSQL connected (after ${5 - retries} retries)`);

      // Initialize pg-boss
      await boss.start();
      console.log('✅ pg-boss queue initialized');
      
    } catch (err) {
      retries--;
      console.log(`⚠️ Connection failed, retries left: ${retries}`, err.message);
      if (retries === 0) {
        console.error('❌ Failed to connect to PostgreSQL');
        process.exit(1);
      }
      await new Promise(r => setTimeout(r, 3000));
    }
  }
}

// =====================================================
// Worker with Enhanced Error Handling
// =====================================================
boss.work('process-job', async (job) => {
  const jobData = job.data;
  console.log(`\n📦 Processing PostgreSQL job ${job.id} (attempt ${job.attempts + 1}/${boss.config.maxRetries})`);
  console.log(`   Content: ${jobData.content_id}`);

  try {
    const envelope = {
      id: `job-${jobData.job_id}-${Date.now()}`,
      timestamp: Date.now() / 1000,
      payload: {
        job_id: jobData.job_id,
        content_id: jobData.content_id,
        creator_id: jobData.creator_id,
        job_type: jobData.job_type,
        params: jobData.params || {},
        submission_id: jobData.submission_id
      },
      schema_version: "tier2-envelope-v1",
      orchestration_policy: "job_processing",
      signature: `tier2-semantic-job-${Date.now()}-${Math.random().toString(36).substring(7)}`,
      metadata: {
        source: "tier5_pg_queue",
        pg_job_id: job.id,
        attempt: job.attempts + 1
      }
    };

    console.log(`   📨 Sending to Tier-4: ${TIER4_URL}`);

    try {
      const tier4Response = await axios.post(`${TIER4_URL}/process-envelope`, envelope, {
        timeout: 30000,
        headers: { 'Content-Type': 'application/json' }
      });

      console.log(`   ✅ Tier-4 responded: ${tier4Response.data.decision || 'unknown'}`);

      // Update job_queue table
      await pgClient.query(
        `UPDATE job_queue
         SET status=$1, completed_at=NOW(), attempts=$2
         WHERE job_id=$3`,
        ['completed', job.attempts + 1, jobData.job_id]
      );

      if (jobData.submission_id) {
        await pgClient.query(
          `UPDATE content_submissions SET status='processing' WHERE submission_id=$1`,
          [jobData.submission_id]
        );
      }

      return tier4Response.data;

    } catch (err) {
      // Handle 429 rate limiting specially
      if (err.response && err.response.status === 429) {
        console.log(`   ⏳ Rate limited (429), will retry...`);
        await pgClient.query(
          `UPDATE job_queue
           SET attempts=$1, failure_reason=$2
           WHERE job_id=$3`,
          [job.attempts + 1, `Rate limited, retry ${job.attempts + 1}/${boss.config.maxRetries}`, jobData.job_id]
        );
        throw err; // Let pg-boss handle retry
      }

      // Other errors
      console.error(`   ❌ Job failed:`, err.message);
      await pgClient.query(
        `UPDATE job_queue
         SET failure_reason=$1, attempts=$2
         WHERE job_id=$3`,
        [err.message, job.attempts + 1, jobData.job_id]
      );
      throw err;
    }

  } catch (err) {
    console.error(`   ❌ Worker error:`, err.message);
    throw err; // pg-boss handles retries
  }
});

// =====================================================
// API Endpoints
// =====================================================
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    tier: 5,
    tier4_url: TIER4_URL,
    pg_queue: boss.isStarted ? 'ready' : 'starting',
    db_connected: true,
    timestamp: new Date().toISOString()
  });
});

app.get('/api/queue/stats', async (req, res) => {
  try {
    const [queued, active, completed, failed] = await Promise.all([
      boss.getQueueSize('process-job'),
      boss.getQueueSize('process-job', { active: true }),
      boss.getQueueSize('process-job', { completed: true }),
      boss.getQueueSize('process-job', { failed: true })
    ]);
    res.json({ 
      waiting: queued, 
      active, 
      completed, 
      failed, 
      total: queued + active + completed + failed 
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/redis-job', async (req, res) => {
  // This endpoint now uses PostgreSQL instead of Redis
  try {
    const { creator_id, content_id, job_type, params, submission_id } = req.body;

    // Insert into pg-boss queue
    const jobId = await boss.send('process-job', {
      creator_id: creator_id || 1,
      content_id: content_id || `job-${Date.now()}`,
      job_type: job_type || 'video',
      params: params || {},
      submission_id,
      created_at: new Date().toISOString()
    }, {
      retryLimit: 5,
      retryBackoff: true,
      priority: req.body.priority || 1
    });

    // Also insert into job_queue table for tracking
    const pgResult = await pgClient.query(
      `INSERT INTO job_queue (creator_id, content_id, job_type, status, params, created_at, attempts)
       VALUES ($1, $2, $3, 'pending', $4, NOW(), 0)
       RETURNING *`,
      [creator_id || 1, content_id || `job-${Date.now()}`, job_type || 'video', params || {}]
    );

    res.json({ 
      success: true, 
      pg_job_id: jobId, 
      pg_job: pgResult.rows[0], 
      message: 'Job added to PostgreSQL queue' 
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/test-job', async (req, res) => {
  try {
    const { creator_id = 1, content_id = `test-${Date.now()}`, job_type = 'video' } = req.body;

    const jobId = await boss.send('process-job', {
      creator_id,
      content_id,
      job_type,
      params: { test: true },
      created_at: new Date().toISOString()
    }, { retryLimit: 5 });

    const result = await pgClient.query(
      `INSERT INTO job_queue (creator_id, content_id, job_type, status, params, created_at, attempts)
       VALUES ($1, $2, $3, 'pending', $4, NOW(), 0)
       RETURNING *`,
      [creator_id, content_id, job_type, { test: true }]
    );

    res.json({ message: 'Test job created', job: result.rows[0], pg_job_id: jobId });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/creator/:id/jobs', async (req, res) => {
  try {
    const results = await pgClient.query(
      'SELECT * FROM job_queue WHERE creator_id = $1 ORDER BY created_at DESC',
      [req.params.id]
    );
    res.json(results.rows);
  } catch (err) {
    res.status(500).json({ error: 'Internal server error' });
  }
});

// =====================================================
// Start server
// =====================================================
const PORT = process.env.PORT || 3001;

app.listen(PORT, async () => {
  console.log(`\n🚀 Tier-5 server running on port ${PORT}`);
  console.log(`📡 Tier-4 URL: ${TIER4_URL}`);
  console.log(`📝 Endpoints:`);
  console.log(`   GET  /health`);
  console.log(`   GET  /creator/:id/jobs`);
  console.log(`   POST /test-job`);
  console.log(`   POST /api/redis-job (PostgreSQL queue)`);
  console.log(`   GET  /api/queue/stats (Queue metrics)`);
  console.log(`   🔄 PostgreSQL queue with 5 concurrent workers`);
  console.log(`   🔄 Exponential backoff enabled`);

  await initDatabase();
});

process.on('SIGTERM', async () => {
  console.log('SIGTERM received, closing connections...');
  await boss.stop();
  await pgClient.end();
  process.exit(0);
});
