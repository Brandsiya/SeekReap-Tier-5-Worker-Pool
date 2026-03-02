-- 1. Wipe the old structure
DROP VIEW IF EXISTS job;
DROP TABLE IF EXISTS submissions CASCADE;
DROP TABLE IF EXISTS submissions_queue CASCADE;

-- 2. Create the clean unified table
CREATE TABLE submissions (
    job_id SERIAL PRIMARY KEY,
    job_type TEXT NOT NULL,
    status TEXT DEFAULT 'pending',
    payload JSONB,
    result JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);

-- 3. Insert a fresh test job
INSERT INTO submissions (job_type, status) VALUES ('reap_init_test', 'pending');
