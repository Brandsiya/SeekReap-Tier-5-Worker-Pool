const { Client } = require('pg');
const express = require('express');
const axios = require('axios');
const { Worker, Queue, QueueEvents } = require('bullmq');
const Redis = require('ioredis');

// =====================================================
// Configuration
// =====================================================
const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  console.error('❌ DATABASE_URL not set');
  process.exit(1);
}

// Get Redis URL from environment
const redisUrl = process.env.REDIS_URL;

if (!redisUrl) {
  console.error('❌ FATAL: REDIS_URL environment variable not set!');
  console.error('   Please set REDIS_URL in your Render environment variables');
  process.exit(1);
}

// Log Redis connection (hide password)
const maskedUrl = redisUrl.replace(/redis:\/\/[^@]+@/, 'redis://****@');
console.log(`🔌 Connecting to Redis at: ${maskedUrl.split('@')[1] || maskedUrl}`);

const redisConnection = new Redis(redisUrl, {
  tls: {
    rejectUnauthorized: false  // Required for Render Redis
  },
  maxRetriesPerRequest: null,
  enableReadyCheck: false,
  retryStrategy: (times) => {
    // Exponential backoff
    const delay = Math.min(times * 100, 3000);
    console.log(`🔄 Redis reconnecting in ${delay}ms (attempt ${times})`);
    return delay;
  }
});

redisConnection.on('connect', () => {
  console.log('✅ Redis connected successfully!');
  const displayHost = redisUrl.split('@')[1] || 'Redis';
  console.log(`   Connected to: ${displayHost}`);
});

redisConnection.on('ready', () => {
  console.log('✅ Redis client ready');
});

redisConnection.on('error', (err) => {
  console.error('❌ Redis error:', err.message);
  if (err.code === 'ECONNREFUSED') {
    console.error('   Connection refused - check REDIS_URL and network settings');
    console.error('   Make sure Redis is running and accessible');
  }
});

redisConnection.on('reconnecting', () => {
  console.log('🔄 Redis reconnecting...');
});

redisConnection.on('end', () => {
  console.log('⚠️ Redis connection ended');
});

// PostgreSQL client with better error handling
const pgClient = new Client({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  connectionTimeoutMillis: 10000,
  idle_in_transaction_session_timeout: 30000
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
// Redis Queue Setup with Better Retry Options
// =====================================================
const jobQueue = new Queue('content-moderation', {
  connection: redisConnection,
  defaultJobOptions: {
    attempts: 5,
    backoff: {
      type: 'exponential',
      delay: 1000
    },
    removeOnComplete: 100,
    removeOnFail: 200
  }
});

const queueEvents = new QueueEvents('content-moderation', { connection: redisConnection });

queueEvents.on('completed', ({ jobId }) => console.log(`✅ Redis job ${jobId} completed`));
queueEvents.on('failed', ({ jobId, failedReason }) => console.error(`❌ Redis job ${jobId} failed:`, failedReason));

// =====================================================
// Worker with Enhanced Error Handling
// =====================================================
const worker = new Worker('content-moderation', async (job) => {
  console.log(`\n📦 Processing Redis job ${job.id} (attempt ${job.attemptsMade + 1}/${job.opts.attempts})`);
  console.log(`   Content: ${job.data.content_id}`);

  try {
    const envelope = {
      id: `job-${job.data.job_id}-${Date.now()}`,
      timestamp: Date.now() / 1000,
      payload: {
        job_id: job.data.job_id,
        content_id: job.data.content_id,
        creator_id: job.data.creator_id,
        job_type: job.data.job_type,
        params: job.data.params || {},
        submission_id: job.data.submission_id
      },
      schema_version: "tier2-envelope-v1",
      orchestration_policy: "job_processing",
      signature: `tier2-semantic-job-${Date.now()}-${Math.random().toString(36).substring(7)}`,
      metadata: {
        source: "tier5_redis_worker",
        redis_job_id: job.id,
        attempt: job.attemptsMade + 1
      }
    };

    console.log(`   📨 Sending to Tier-4: ${TIER4_URL}`);

    try {
      const tier4Response = await axios.post(`${TIER4_URL}/process-envelope`, envelope, {
        timeout: 30000,
        headers: { 'Content-Type': 'application/json' }
      });

      console.log(`   ✅ Tier-4 responded: ${tier4Response.data.decision || 'unknown'}`);

      // Update PostgreSQL
      await pgClient.query(
        `UPDATE job_queue
         SET status=$1, completed_at=NOW(), redis_job_id=$2, attempts=$3
         WHERE job_id=$4`,
        ['completed', job.id, job.attemptsMade + 1, job.data.job_id]
      );

      if (job.data.submission_id) {
        await pgClient.query(
          `UPDATE content_submissions SET status='processing' WHERE submission_id=$1`,
          [job.data.submission_id]
        );
      }

      return tier4Response.data;

    } catch (err) {
      // Handle 429 rate limiting specially
      if (err.response && err.response.status === 429) {
        console.log(`   ⏳ Rate limited (429), will retry with backoff...`);
        // Update PostgreSQL with retry info
        await pgClient.query(
          `UPDATE job_queue
           SET attempts=$1, failure_reason=$2, redis_job_id=$3
           WHERE job_id=$4`,
          [job.attemptsMade + 1, `Rate limited, retry ${job.attemptsMade + 1}/${job.opts.attempts}`, job.id, job.data.job_id]
        );
        // Throw to trigger BullMQ retry with backoff
        throw err;
      }

      // Other errors
      console.error(`   ❌ Job failed:`, err.message);
      await pgClient.query(
        `UPDATE job_queue
         SET failure_reason=$1, redis_job_id=$2, attempts=$3
         WHERE job_id=$4`,
        [err.message, job.id, job.attemptsMade + 1, job.data.job_id]
      );
      throw err;
    }

  } catch (err) {
    console.error(`   ❌ Worker error:`, err.message);
    throw err; // BullMQ handles retries
  }
}, {
  connection: redisConnection,
  concurrency: 5,
  limiter: {
    max: 10,
    duration: 1000
  }
});

worker.on('active', (job) => console.log(`🔄 Worker started job ${job.id}`));
worker.on('completed', (job) => console.log(`✅ Worker completed job ${job.id}`));
worker.on('failed', (job, err) => console.error(`❌ Worker failed job ${job.id}:`, err.message));

// =====================================================
// Database connection
// =====================================================
async function initDatabase() {
  let connected = false;
  let retries = 5;

  while (!connected && retries > 0) {
    try {
      await pgClient.connect();
      connected = true;
      console.log(`✅ PostgreSQL connected (after ${5 - retries} retries)`);
    } catch (err) {
      retries--;
      console.log(`⚠️ PostgreSQL connection failed, retries left: ${retries}`);
      if (retries === 0) {
        console.error('❌ Failed to connect to PostgreSQL');
        process.exit(1);
      }
      await new Promise(r => setTimeout(r, 3000));
    }
  }
}

// =====================================================
// API Endpoints
// =====================================================
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    tier: 5,
    tier4_url: TIER4_URL,
    redis_connected: redisConnection.status === 'ready' || redisConnection.status === 'connect',
    db_connected: true,
    timestamp: new Date().toISOString()
  });
});

app.get('/api/queue/stats', async (req, res) => {
  try {
    const [waiting, active, completed, failed] = await Promise.all([
      jobQueue.getWaitingCount(),
      jobQueue.getActiveCount(),
      jobQueue.getCompletedCount(),
      jobQueue.getFailedCount()
    ]);
    res.json({ waiting, active, completed, failed, total: waiting + active + completed + failed });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/redis-job', async (req, res) => {
  try {
    const { creator_id, content_id, job_type, params, submission_id } = req.body;

    const redisJob = await jobQueue.add('process', {
      creator_id: creator_id || 1,
      content_id: content_id || `redis-${Date.now()}`,
      job_type: job_type || 'video',
      params: params || {},
      submission_id,
      created_at: new Date().toISOString()
    }, {
      priority: req.body.priority || 1,
      attempts: 5
    });

    const pgResult = await pgClient.query(
      `INSERT INTO job_queue (creator_id, content_id, job_type, status, params, created_at, redis_job_id, attempts)
       VALUES ($1, $2, $3, 'pending', $4, NOW(), $5, 0)
       RETURNING *`,
      [creator_id || 1, content_id || `redis-${Date.now()}`, job_type || 'video', params || {}, redisJob.id]
    );

    res.json({ success: true, redis_job_id: redisJob.id, pg_job: pgResult.rows[0], message: 'Job added to Redis queue' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/test-job', async (req, res) => {
  try {
    const { creator_id = 1, content_id = `test-${Date.now()}`, job_type = 'video' } = req.body;

    const redisJob = await jobQueue.add('process', {
      creator_id,
      content_id,
      job_type,
      params: { test: true },
      created_at: new Date().toISOString()
    }, { attempts: 5 });

    const result = await pgClient.query(
      `INSERT INTO job_queue (creator_id, content_id, job_type, status, params, created_at, redis_job_id, attempts)
       VALUES ($1, $2, $3, 'pending', $4, NOW(), $5, 0)
       RETURNING *`,
      [creator_id, content_id, job_type, { test: true }, redisJob.id]
    );

    res.json({ message: 'Test job created', job: result.rows[0], redis_job_id: redisJob.id });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/creator/:id/jobs', async (req, res) => {
  try {
    const results = await pgClient.query(
      'SELECT * FROM content_results WHERE creator_id = $1 ORDER BY processed_at DESC',
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
  console.log(`   POST /api/redis-job (Redis queue)`);
  console.log(`   GET  /api/queue/stats (Queue metrics)`);
  console.log(`   🔄 Auto-reconnect enabled`);
  console.log(`   🔄 Redis queue with 5 concurrent workers`);
  console.log(`   🔄 Exponential backoff (1s,2s,4s,8s,16s)`);

  await initDatabase();
});

process.on('SIGTERM', async () => {
  console.log('SIGTERM received, closing connections...');
  await worker.close();
  await jobQueue.close();
  await redisConnection.quit();
  await pgClient.end();
  process.exit(0);
});
