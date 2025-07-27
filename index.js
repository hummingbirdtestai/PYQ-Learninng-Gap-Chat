// index.js
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const swaggerUi = require('swagger-ui-express');
const YAML = require('yamljs');

const app = express();

// ✅ CORS Configuration
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'PUT', 'DELETE'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

// ✅ JSON Parsing Middleware
app.use(express.json());

// ✅ Swagger Docs Loader
const swaggerDoc = YAML.load('./docs/swagger.yaml');
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerDoc));

// ✅ Route Registrations
app.use('/auth', require('./routes/auth.routes'));           // Twilio OTP Auth
app.use('/users', require('./routes/user.routes'));          // User Registration, Status
app.use('/colleges', require('./routes/college.routes'));    // Medical Collegess
app.use('/exams', require('./routes/examRoutes'));          // Exam + Subject APIs

// ✅ Start Express Server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
