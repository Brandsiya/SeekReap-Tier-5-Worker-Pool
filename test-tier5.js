const axios = require('axios');

const TIER5_URL = process.env.TIER5_URL || 'http://localhost:3000';

async function testTier5() {
  console.log('=== Testing Tier-5 Worker Pool ===\n');
  
  // Test health
  try {
    const health = await axios.get(`${TIER5_URL}/health`);
    console.log('✅ Health check:', health.data);
  } catch (error) {
    console.log('❌ Health check failed:', error.message);
    return;
  }
  
  // Create a test job
  try {
    console.log('\n📝 Creating test job...');
    const job = await axios.post(`${TIER5_URL}/test-job`, {
      creator_id: 1,
      content_id: `test-content-${Date.now()}`,
      job_type: 'video'
    });
    console.log('✅ Test job created:', job.data.job.job_id);
    console.log('   Status:', job.data.job.status);
  } catch (error) {
    console.log('❌ Failed to create test job:', error.message);
  }
  
  // Wait a bit for processing
  console.log('\n⏳ Waiting for worker to process job...');
  await new Promise(r => setTimeout(r, 5000));
  
  // Check results
  try {
    console.log('\n📊 Checking results for creator 1...');
    const results = await axios.get(`${TIER5_URL}/creator/1/jobs`);
    console.log(`✅ Found ${results.data.length} processed jobs`);
    if (results.data.length > 0) {
      const latest = results.data[0];
      console.log('\n   Latest job:');
      console.log('   Job ID:', latest.job_id);
      console.log('   Decision:', latest.decision);
      console.log('   Confidence:', latest.confidence);
      console.log('   Appeal:', latest.appeal_text);
    }
  } catch (error) {
    console.log('❌ Failed to fetch results:', error.message);
  }
}

testTier5();
