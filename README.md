# 🐝 SeekReap Tier-5 Worker Pool
**Core Role:** The execution layer. Handles high-CPU tasks directed by Tier-4.

## 🛠 Setup & Runtime
1. **Primary Entry Point:** `node start.js` (Runs on Port 3001)
2. **Web Entry Point:** `node server.js` (Runs on Port 3000)

## 📡 Ports & Services
- **3001:** Core Job Worker & Queue Stats.
- **3000:** General API Interface.

## 🧪 Health Check
- Worker Health: `curl http://localhost:3001/health`
# CI/CD test Sun Mar  8 21:23:48 UTC 2026
