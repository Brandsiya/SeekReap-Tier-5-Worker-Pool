const { Client } = require('pg');
const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) { console.error('DATABASE_URL is not set'); process.exit(1); }

const client = new Client({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: false } });

async function processJob(job) {
    console.log(`Processing job ${job.job_id}: ${job.content_id}`);
    try {
        await new Promise(r => setTimeout(r, 2000)); // simulate work
        await client.query(
            'UPDATE job_queue SET status=$1, completed_at=NOW() WHERE job_id=$2',
            ['completed', job.job_id]
        );
        console.log(`Job ${job.job_id} completed`);
    } catch (err) {
        console.error(`Job ${job.job_id} failed`, err);
        await client.query(
            'UPDATE job_queue SET status=$1 WHERE job_id=$2',
            ['failed', job.job_id]
        );
    }
}

async function runWorker() {
    await client.connect();
    console.log('Worker connected to DB');
    while (true) {
        const res = await client.query(
            "SELECT job_id, content_id FROM job_queue WHERE status='pending' ORDER BY job_id ASC LIMIT 1"
        );
        if (res.rows.length === 0) { await new Promise(r => setTimeout(r, 2000)); continue; }
        await client.query("UPDATE job_queue SET status='in_progress', started_at=NOW() WHERE job_id=$1", [res.rows[0].job_id]);
        await processJob(res.rows[0]);
    }
}

runWorker().catch(err => { console.error('Worker error', err); process.exit(1); });
