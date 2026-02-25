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

// Redis connection from Render
const REDIS_CONFIG = {
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT || '6379'),
  password: process.env.REDIS_PASSWORD,
  maxRetriesPerRequest: null,
  enableReadyCheck: false,
  retryStrategy: (times) => {
    return Math.min(times * 50, 2000);
  }
};

console.log('🔄 Connecting to Redis...');
const redisConnection = new Redis(REDIS_CONFIG);

redisConnection.on('connect', () => {
  console.log('✅ Redis connected successfully');
});

redisConnection.on('error', (err) => {
  console.error('❌ Redis connection error:', err.message);
});

// PostgreSQL client
const pgClient = new Client({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// Express server
const app = express();
app.use(express.json());

// Configuration
const TIER4_URL = process.env.TIER4_URL || 'http://localhost:10000';

// =====================================================
// Redis Queue Setup
// =====================================================
const jobQueue = new Queue('content-moderation', {
  connection: redisConnection,
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: 'exponential',
      delay: 1000
    },
    removeOnComplete: 100,
    removeOnFail: 200
  }
});

const queueEvents = new QueueEvents('content-moderation', {
  connection: redisConnection
});

// Monitor queue events
queueEvents.on('completed', ({ jobId }) => {
  console.log(`✅ Redis job ${jobId} completed`);
});

queueEvents.on('failed', ({ jobId, failedReason }) => {
  console.error(`❌ Redis job ${jobId} failed:`, failedReason);
});

// =====================================================
// Worker Setup
// =====================================================
const worker = new Worker('content-moderation', async (job) => {
  console.log(`\n📦 Processing Redis job ${job.id}: ${job.data.content_id}`);
  console.log(`   Attempt ${job.attemptsMade + 1}/${job.opts.attempts}`);
  
  try {
    // Create envelope
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
        redis_job_id: job.id
      }
    };

    console.log(`   📨 Sending to Tier-4: ${TIER4_URL}`);

    // Send to Tier-4
    const tier4Response = await axios.post(`${TIER4_URL}/process-envelope`, envelope, {
      timeout: 30000,
      headers: { 'Content-Type': 'application/json' }
    });

    console.log(`   ✅ Tier-4 responded: ${tier4Response.data.decision || 'unknown'}`);

    // Update PostgreSQL job status
    await pgClient.query(
      `UPDATE job_queue 
       SET status=$1, completed_at=NOW(), redis_job_id=$2 
       WHERE job_id=$3`,
      ['completed', job.id, job.data.job_id]
    );

    // If there's a submission_id, update content_submissions
    if (job.data.submission_id) {
      await pgClient.query(
        `UPDATE content_submissions 
         SET status='processing' 
         WHERE submission_id=$1`,
        [job.data.submission_id]
      );
    }

    return tier4Response.data;

  } catch (err) {
    console.error(`   ❌ Job ${job.id} failed:`, err.message);
    
    // Update PostgreSQL with failure
    await pgClient.query(
      `UPDATE job_queue 
       SET failure_reason=$1, redis_job_id=$2 
       WHERE job_id=$3`,
      [err.message, job.id, job.data.job_id]
    );
    
    throw err; // BullMQ will handle retries
  }
}, {
  connection: redisConnection,
  concurrency: 5, // Process 5 jobs in parallel
  limiter: {
    max: 10,      // Max 10 jobs per second
    duration: 1000
  }
});

worker.on('active', (job) => {
  console.log(`🔄 Worker started job ${job.id}`);
});

worker.on('completed', (job) => {
  console.log(`✅ Worker completed job ${job.id}`);
});

worker.on('failed', (job, err) => {
  console.error(`❌ Worker failed job ${job.id}:`, err.message);
});

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
      console.log(`✅ PostgreSQL connected (after ${5-retries} retries)`);
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

// Health check
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    tier: 5,
    tier4_url: TIER4_URL,
    redis_connected: redisConnection.status === 'ready',
    db_connected: true,
    timestamp: new Date().toISOString()
  });
});

// Get queue stats
app.get('/api/queue/stats', async (req, res) => {
  try {
    const [waiting, active, completed, failed] = await Promise.all([
      jobQueue.getWaitingCount(),
      jobQueue.getActiveCount(),
      jobQueue.getCompletedCount(),
      jobQueue.getFailedCount()
    ]);
    
    res.json({
      waiting,
      active,
      completed,
      failed,
      total: waiting + active + completed + failed
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Add job to Redis queue (instead of PostgreSQL)
app.post('/api/redis-job', async (req, res) => {
  try {
    const { creator_id, content_id, job_type, params, submission_id } = req.body;
    
    // Add to Redis queue
    const redisJob = await jobQueue.add('process', {
      creator_id: creator_id || 1,
      content_id: content_id || `redis-${Date.now()}`,
      job_type: job_type || 'video',
      params: params || {},
      submission_id,
      created_at: new Date().toISOString()
    }, {
      priority: req.body.priority || 1,
      attempts: req.body.attempts || 3
    });
    
    // Also add to PostgreSQL for persistence
    const pgResult = await pgClient.query(
      `INSERT INTO job_queue (creator_id, content_id, job_type, status, params, created_at, redis_job_id)
       VALUES ($1, $2, $3, 'pending', $4, NOW(), $5)
       RETURNING *`,
      [creator_id || 1, content_id || `redis-${Date.now()}`, job_type || 'video', params || {}, redisJob.id]
    );
    
    res.json({
      success: true,
      redis_job_id: redisJob.id,
      pg_job: pgResult.rows[0],
      message: 'Job added to Redis queue'
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Test endpoint
app.post('/test-job', async (req, res) => {
  try {
    const { creator_id = 1, content_id = `test-${Date.now()}`, job_type = 'video' } = req.body;
    
    // Add to Redis queue
    const redisJob = await jobQueue.add('process', {
      creator_id,
      content_id,
      job_type,
      params: { test: true },
      created_at: new Date().toISOString()
    });
    
    // Add to PostgreSQL
    const result = await pgClient.query(
      `INSERT INTO job_queue (creator_id, content_id, job_type, status, params, created_at, redis_job_id)
       VALUES ($1, $2, $3, 'pending', $4, NOW(), $5)
       RETURNING *`,
      [creator_id, content_id, job_type, { test: true }, redisJob.id]
    );
    
    res.json({
      message: 'Test job created',
      job: result.rows[0],
      redis_job_id: redisJob.id,
      note: 'Worker will process this job via Redis'
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get jobs from PostgreSQL (for backward compatibility)
app.get('/creator/:id/jobs', async (req, res) => {
  try {
    const results = await pgClient.query(
      'SELECT * FROM content_results WHERE creator_id = $1 ORDER BY processed_at DESC',
      [req.params.id]
    );
    res.json(results.rows);
  } catch (err) {
    console.error('Error fetching jobs:', err);
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
  console.log(`   POST /api/redis-job (NEW - Redis queue)`);
  console.log(`   GET  /api/queue/stats (NEW - Queue metrics)`);
  console.log(`   🔄 Auto-reconnect enabled`);
  console.log(`   🔄 Redis queue with 5 concurrent workers`);
  
  await initDatabase();
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM received, closing connections...');
  await worker.close();
  await jobQueue.close();
  await redisConnection.quit();
  await pgClient.end();
  process.exit(0);
});
