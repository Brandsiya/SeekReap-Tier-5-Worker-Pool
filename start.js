const { Client } = require('pg');
const express = require('express');
const axios = require('axios');

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  console.error('DATABASE_URL not set');
  process.exit(1);
}

// Express server
const app = express();
app.use(express.json());

// Configuration
const TIER4_URL = process.env.TIER4_URL || 'http://localhost:10000';

// Global client variable
let client;

// Function to create and connect a new client
async function createClient() {
  const newClient = new Client({
    connectionString: DATABASE_URL,
    ssl: { rejectUnauthorized: false },
    // Add connection timeout and keep-alive
    connectionTimeoutMillis: 10000,
    idle_in_transaction_session_timeout: 30000
  });
  
  await newClient.connect();
  console.log('🔌 New database connection established');
  
  // Handle errors on this client
  newClient.on('error', (err) => {
    console.error('Database client error:', err.message);
    console.log('Attempting to reconnect...');
    // Don't exit, just mark as disconnected
    client = null;
  });
  
  return newClient;
}

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
    
    // Ensure we have a database connection
    if (!client) {
      client = await createClient();
    }
    
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
    
    // Try to mark job as failed, but if DB is down, just log it
    try {
      if (client) {
        await client.query(
          'UPDATE job_queue SET status=$1, failure_reason=$2 WHERE job_id=$3',
          ['failed', err.message, job.job_id]
        );
      }
    } catch (dbErr) {
      console.error('   Could not update job status in DB:', dbErr.message);
    }
  }
}

async function runWorker() {
  // Initial connection
  try {
    client = await createClient();
    console.log('✅ Initial database connection established');
  } catch (err) {
    console.error('❌ Failed to connect to database:', err.message);
    process.exit(1);
  }
  
  console.log(`🌐 Tier-4 URL: ${TIER4_URL}\n`);
  
  while (true) {
    try {
      // Check if we have a valid connection
      if (!client) {
        console.log('⚠️ Database connection lost, reconnecting...');
        client = await createClient();
      }
      
      // Test the connection with a simple query
      await client.query('SELECT 1');
      
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
      
      // If database error, reset client
      if (err.code === 'ECONNREFUSED' || err.code === 'ECONNABORTED' || err.code === '57P01') {
        console.log('⚠️ Database connection issue, will reconnect on next iteration');
        try {
          await client?.end();
        } catch (e) {}
        client = null;
      }
      
      await new Promise(r => setTimeout(r, 5000));
    }
  }
}

// ---------------- Server ----------------
app.get('/creator/:id/jobs', async (req, res) => {
  try {
    if (!client) {
      client = await createClient();
    }
    
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
    db_connected: !!client,
    timestamp: new Date().toISOString()
  });
});

// Test endpoint to create a test job
app.post('/test-job', async (req, res) => {
  try {
    if (!client) {
      client = await createClient();
    }
    
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
    console.error('Error creating test job:', err);
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`🚀 Tier-5 server running on port ${PORT}`);
  console.log(`📡 Tier-4 URL: ${TIER4_URL}`);
  console.log(`📝 Endpoints:`);
  console.log(`   GET  /health`);
  console.log(`   GET  /creator/:id/jobs`);
  console.log(`   POST /test-job`);
  console.log(`   🔄 Auto-reconnect enabled for database`);
});

// Run worker asynchronously
runWorker().catch(err => { 
  console.error('Worker error', err); 
  // Don't exit immediately on worker error
  setTimeout(() => process.exit(1), 1000);
});
