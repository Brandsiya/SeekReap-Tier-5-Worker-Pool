# 🐝 SeekReap Tier-5 Worker Pool
**Core Role:** The execution layer. This pool handles high-CPU/Memory tasks (Video/Data) as directed by the Tier-4 Orchestrator.

## 🛠 Setup & Runtime
1. **Install:** `npm install`
2. **Execution:** `node start.js`
3. **Entry Point:** start.js (Initializes worker_pool.js)

## 📡 Integration
- **Inbound:** Receives delegated tasks from Tier-4 Orchestrator.
- **Outbound:** Returns processed results to the Tier-3 Core Engine.

## 🧪 Health
Verify worker availability: `curl http://localhost:PORT/health`
