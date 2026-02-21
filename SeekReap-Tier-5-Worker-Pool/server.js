// server.js
const express = require('express');
const app = express();

app.get('/', (req, res) => {
  res.send('Tier-5 server is live!');
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));

app.get('/creator/:id/jobs', async (req, res) => {
  try {
    const results = await client.query(
      'SELECT * FROM content_results WHERE creator_id =  ORDER BY created_at DESC',
      [req.params.id]
    );
    res.json(results.rows);
  } catch (err) {
    console.error('Error fetching jobs', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

