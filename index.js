// index.js

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const swaggerUi = require('swagger-ui-express');
const YAML = require('yamljs');

const app = express();

// ✅ Load Swagger Docs
const swaggerDocument = YAML.load('./docs/swagger.yaml');

// ✅ Import Custom Routes
const importRoutes = require('./routes/import.routes');
const authRoutes = require('./routes/auth.routes');
const userRoutes = require('./routes/user.routes');
const collegeRoutes = require('./routes/college.routes');
const examRoutes = require('./routes/exam.routes');
const generationRoutes = require('./routes/generation.routes'); // GPT Worker Status APIs
const adaptiveRoutes = require('./routes/adaptive.routes');
const mcqGeneratorRoutes = require('./routes/mcqGenerator.routes'); // On-demand MCQ generation
const mcqRoutes = require('./routes/mcq.routes');
const graphRoutes = require('./routes/graphs.routes'); 
const briefingRoutes = require('./routes/briefing.routes'); // Daily briefing

// ✅ Middleware: CORS (allow all origins for now)
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

// ✅ Explicit headers for OPTIONS requests (important for Railway + Expo frontend)
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,PATCH,OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    return res.sendStatus(200);
  }
  next();
});

// ✅ Middleware: JSON Body Parser
app.use(express.json({ limit: '10mb' }));

// ✅ Health check (basic endpoint to test Railway deployment)
app.get('/', (_req, res) =>
  res.json({
    ok: true,
    service: 'HB Backend',
    env: process.env.NODE_ENV || 'dev'
  })
);

// ✅ Swagger Documentation Route
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerDocument));

// ✅ Register API Routes
app.use('/auth', authRoutes);                     // Twilio OTP Auth
app.use('/users', userRoutes);                    // User Registration + Activation
app.use('/colleges', collegeRoutes);              // Medical Colleges List
app.use('/api/exams', examRoutes);                // Exams and Subjects
app.use('/api', importRoutes);                    // Google Sheets → Supabase MCQ Import
app.use('/generation', generationRoutes);         // GPT Worker Status Dashboard
app.use('/api', adaptiveRoutes);                  // Adaptive MCQ APIs
app.use('/api', mcqGeneratorRoutes);              // MCQ generation
app.use('/api', mcqRoutes);                       // MCQ CRUD
app.use('/api', graphRoutes);   
app.use('/api', briefingRoutes);                  // Daily Briefing (→ /api/daily-briefing)

// ✅ Start Express Server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
