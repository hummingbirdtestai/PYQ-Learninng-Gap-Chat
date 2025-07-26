// index.js
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const swaggerUi = require('swagger-ui-express');
const YAML = require('yamljs');

const app = express();

// ✅ CORS Configuration — allow frontend to call API
app.use(cors({
  origin: '*', // Or restrict to frontend: "https://your-frontend-domain"
  methods: ['GET', 'POST', 'PUT', 'DELETE'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

// ✅ JSON Parsing Middleware
app.use(express.json());

// ✅ Load Swagger Docs
const swaggerDoc = YAML.load('./docs/swagger.yaml');
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerDoc));

// ✅ Active Routes
app.use('/auth', require('./routes/auth.routes'));
app.use('/users', require('./routes/user.routes'));

// ❌ Commented-out routes (enable when ready)
// app.use('/mcqs', require('./routes/mcq.routes'));
// app.use('/topics', require('./routes/topic.routes'));
// app.use('/leaderboard', require('./routes/leaderboard.routes'));
// app.use('/ai', require('./routes/ai.routes'));

// ✅ Server Start
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
