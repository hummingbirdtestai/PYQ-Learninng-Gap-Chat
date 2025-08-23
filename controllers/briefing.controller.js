const { supabase } = require('../config/supabaseClient');
const { openai } = require('../config/openaiClient');
const { getStudentFacts } = require('../services/student.service');

exports.getDailyBriefing = async (req, res) => {
  const studentId = req.user.id; // from JWT/session middleware
  const today = new Date().toISOString().slice(0, 10);

  // 1. Check cache
  const { data: cached } = await supabase
    .from('student_daily_briefings')
    .select('*')
    .eq('student_id', studentId)
    .eq('briefing_date', today)
    .single();

  if (cached) return res.json({ message: cached.message });

  // 2. Gather facts
  const facts = await getStudentFacts(studentId);

  // 3. Try GPT
  try {
    const completion = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [
        { role: "system", content: "You are a friendly AI mentor for NEETPG students." },
        { role: "user", content: `Facts: ${JSON.stringify(facts)}. Generate a 2–3 sentence inspiring briefing.` }
      ],
      max_tokens: 120
    });

    const message = completion.choices[0].message.content;

    // 4. Store in cache
    await supabase.from('student_daily_briefings').insert({
      student_id: studentId,
      briefing_date: today,
      message
    });

    return res.json({ message });

  } catch (err) {
    console.error("GPT failed, fallback:", err.message);

    const fallback = `Yesterday you solved ${facts.gaps_closed} questions in ${facts.subject}.
    Today's checkpoint → ${facts.next_topic} (${facts.target_questions} questions).
    Keep pushing 🚀`;

    return res.json({ message: fallback });
  }
};
