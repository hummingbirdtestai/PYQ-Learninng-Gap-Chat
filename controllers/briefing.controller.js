const { supabase } = require('../config/supabaseClient');
const { openai } = require('../config/openaiClient');
const { getStudentFacts } = require('../services/student.service');
const { DAILY_BRIEFING_PROMPT } = require('../config/prompts');

exports.getDailyBriefing = async (req, res) => {
  const studentId = req.user.id; // from JWT/session middleware
  const today = new Date().toISOString().slice(0, 10);

  try {
    // 1. Check cache first
    const { data: cached } = await supabase
      .from('student_daily_briefings')
      .select('*')
      .eq('student_id', studentId)
      .eq('briefing_date', today)
      .single();

    if (cached) {
      return res.json({ message: cached.message });
    }

    // 2. Gather facts from DB/service
    const facts = await getStudentFacts(studentId);

    // 3. Generate briefing via GPT
    const completion = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [
        { role: "system", content: "You are a mentor for NEETPG students." },
        { role: "user", content: DAILY_BRIEFING_PROMPT(facts) }
      ],
      max_tokens: 120
    });

    const message = completion.choices[0].message.content;

    // 4. Store in cache for rest of the day
    await supabase.from('student_daily_briefings').insert({
      student_id: studentId,
      briefing_date: today,
      message
    });

    return res.json({ message });

  } catch (err) {
    console.error("❌ Daily Briefing error:", err);

    // 5. Fallback (safe default if GPT fails)
    const fallbackFacts = await getStudentFacts(studentId); // ensure we have something
    const fallback = `Good day, ${fallbackFacts.student_name || "Doctor"}! 
Yesterday you solved ${fallbackFacts.gaps_closed} questions in ${fallbackFacts.subject}. 
Today's checkpoint → ${fallbackFacts.next_topic} (${fallbackFacts.target_questions} questions). 
Keep pushing 🚀`;

    return res.json({ message: fallback });
  }
};
