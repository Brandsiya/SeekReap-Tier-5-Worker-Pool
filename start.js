const { Client } = require('pg');
const express = require('express');
const axios = require('axios');

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

// Configuration
const TIER4_URL = process.env.TIER4_URL || 'http://localhost:10000';

// ---------------- Worker ----------------
async function processJob(job) {
  console.log(`\n📦 Processing job ${job.job_id}: ${job.content_id}`);
  
  try {
    // Create envelope from job data
    const envelope = {
      id: `job-${job.job_id}-${Date.now()}`,
      timestamp: Date.now() / 1000,
      payload: {
        job_id: job.job_id,
        content_id: job.content_id,
        creator_id: job.creator_id,
        job_type: job.job_type,
        params: job.params || {}
      },
      schema_version: "tier2-envelope-v1",
      orchestration_policy: "job_processing",
      signature: `tier2-semantic-job-${Date.now()}-${Math.random().toString(36).substring(7)}`,
      metadata: {
        source: "tier5_worker",
        job_type: job.job_type
      }
    };
    
    console.log(`   📨 Sending envelope to Tier-4: ${envelope.id}`);
    
    // Send to Tier-4 for processing through the pipeline
    const tier4Response = await axios.post(`${TIER4_URL}/process-envelope`, envelope, {
      timeout: 30000,
      headers: { 'Content-Type': 'application/json' }
    });
    
    console.log(`   ✅ Tier-4 responded with decision: ${tier4Response.data.decision}`);
    console.log(`   📊 Confidence: ${tier4Response.data.confidence}`);
    console.log(`   📝 Appeal: ${tier4Response.data.appeal_text || 'None'}`);
    
    // Store results in database
    const result = await client.query(
      `INSERT INTO content_results 
       (job_id, creator_id, job_type, decision, confidence, risk_factors, appeal_text, processed_at) 
       VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
       RETURNING *`,
      [
        job.job_id, 
        job.creator_id, 
        job.job_type,
        tier4Response.data.decision,
        tier4Response.data.confidence,
        JSON.stringify(tier4Response.data.risk_factors || []),
        tier4Response.data.appeal_text || null
      ]
    );
    
    console.log(`   💾 Results stored in database for job ${job.job_id}`);
    
    // Mark job as completed
    await client.query(
      'UPDATE job_queue SET status=$1, completed_at=NOW() WHERE job_id=$2',
      ['completed', job.job_id]
    );
    
    console.log(`   ✅ Job ${job.job_id} completed successfully\n`);
    
  } catch (err) {
    console.error(`   ❌ Job ${job.job_id} failed:`, err.message);
    if (err.response) {
      console.error('   Response data:', err.response.data);
    }
    
    // Mark job as failed
    await client.query(
      'UPDATE job_queue SET status=$1, failure_reason=$2 WHERE job_id=$3',
      ['failed', err.message, job.job_id]
    );
  }
}

async function runWorker() {
  await client.connect();
  console.log('🔌 Worker connected to DB');
  console.log(`🌐 Tier-4 URL: ${TIER4_URL}\n`);
  
  while (true) {
    try {
      const res = await client.query(
        "SELECT job_id, creator_id, content_id, job_type, params FROM job_queue WHERE status='pending' ORDER BY job_id ASC LIMIT 1"
      );

      if (res.rows.length === 0) {
        // No pending jobs, wait a bit
        await new Promise(r => setTimeout(r, 2000));
        continue;
      }

      const job = res.rows[0];
      
      // Mark as in_progress
      await client.query(
        "UPDATE job_queue SET status='in_progress', started_at=NOW() WHERE job_id=$1",
        [job.job_id]
      );

      await processJob(job);
      
    } catch (err) {
      console.error('Worker loop error:', err.message);
      await new Promise(r => setTimeout(r, 5000));
    }
  }
}

// ---------------- Server ----------------
app.get('/creator/:id/jobs', async (req, res) => {
  try {
    const results = await client.query(
      'SELECT * FROM content_results WHERE creator_id = $1 ORDER BY processed_at DESC',
      [req.params.id]
    );
    res.json(results.rows);
  } catch (err) {
    console.error('Error fetching jobs', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy', 
    tier: 5,
    tier4_url: TIER4_URL,
    timestamp: new Date().toISOString()
  });
});

// Test endpoint to create a test job
app.post('/test-job', async (req, res) => {
  try {
    const { creator_id = 1, content_id = `test-${Date.now()}`, job_type = 'video' } = req.body;
    
    const result = await client.query(
      `INSERT INTO job_queue (creator_id, content_id, job_type, status, params, created_at)
       VALUES ($1, $2, $3, $4, $5, NOW())
       RETURNING *`,
      [creator_id, content_id, job_type, 'pending', { test: true }]
    );
    
    res.json({ 
      message: 'Test job created', 
      job: result.rows[0],
      note: 'Worker will process this job automatically'
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Tier-5 server running on port ${PORT}`);
  console.log(`📡 Tier-4 URL: ${TIER4_URL}`);
  console.log(`📝 Endpoints:`);
  console.log(`   GET  /health`);
  console.log(`   GET  /creator/:id/jobs`);
  console.log(`   POST /test-job`);
});

// Run worker asynchronously
runWorker().catch(err => { 
  console.error('Worker error', err); 
  process.exit(1); 
});
