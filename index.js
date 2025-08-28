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
const generationRoutes = require('./routes/generation.routes'); 
const adaptiveRoutes = require('./routes/adaptive.routes');
const mcqGeneratorRoutes = require('./routes/mcqGenerator.routes'); 
const mcqRoutes = require('./routes/mcq.routes');
const graphRoutes = require('./routes/graphs.routes'); 
const briefingRoutes = require('./routes/briefing.routes');// ✅ separate file
const progressRoutes = require("./routes/progress.routes");
const answersRoutes = require("./routes/answers.routes");

// ✅ Middleware: CORS
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,PATCH,OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    return res.sendStatus(200);
  }
  next();
});

// ✅ Middleware: JSON parser
app.use(express.json({ limit: '10mb' }));

// ✅ Health check
app.get('/', (_req, res) =>
  res.json({
    ok: true,
    service: 'HB Backend',
    env: process.env.NODE_ENV || 'dev'
  })
);

// ✅ Swagger docs
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerDocument));

// ✅ Register API Routes
app.use('/auth', authRoutes);                     
app.use('/users', userRoutes);                    
app.use('/colleges', collegeRoutes);              
app.use('/api/exams', examRoutes);                
app.use('/api', importRoutes);                    
app.use('/generation', generationRoutes);         
app.use('/api', adaptiveRoutes);                  
app.use('/api', mcqGeneratorRoutes);              
app.use('/api', mcqRoutes);                       
app.use('/api', graphRoutes);   
app.use('/api', briefingRoutes); // ✅ /api/daily-briefing
app.use("/api/student", progressRoutes);
app.use("/api/student", answersRoutes);

// ✅ Start Express Server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
