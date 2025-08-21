// ===== Imports =====
const { createClient } = require('@supabase/supabase-js');
const OpenAI = require('openai');

// ✅ Init Supabase (Railway uses SUPABASE_KEY)
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

// ✅ Init OpenAI
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY
});

// ===== Graphs Cleaner Prompt =====
const GRAPHS_PROMPT_TEMPLATE = `
You are an expert teacher.  
I will give you raw study material (text or images).  

Your task:  
1. Reorganize the content into **valid JSON only**.  
2. Use an **array of objects**.  
3. Each object must have exactly 2 keys:  
   - "ConceptTitle" = short, clear title of the concept.  
   - "Explanation" = explanation of the concept, written exactly as in the text.  
4. Use **Markdown bold** (**word**) for highlighting important terms, numbers, formulas, names, etc.  
5. Do not skip any content from the input.  
6. Final output must be inside a **code block** as JSON (no extra text outside).  

### Example Input:
Types of Pure Chemicals: Pure chemicals are mainly of two types: elements and compounds.  

### Example Output:
\`\`\`json
[
  {
    "ConceptTitle": "Types of Pure Chemicals",
    "Explanation": "Pure chemicals are mainly of two types: **elements** and **compounds**."
  }
]
\`\`\`
`;

const G_MODEL = process.env.G_MODEL || 'gpt-5-mini';
const G_HTTP_CONCURRENCY = parseInt(process.env.G_HTTP_CONCURRENCY || '3', 10);

// ===== Helpers =====
function gCleanAndParseJSON(raw) {
  let t = String(raw || '').trim()
    .replace(/^```json\s*/i, '')
    .replace(/^```/i, '')
    .replace(/```$/,'')
    .trim();
  return JSON.parse(t);
}

async function gAsyncPool(limit, items, iter) {
  const out = [];
  const exec = [];
  for (const it of items) {
    const p = Promise.resolve().then(() => iter(it));
    out.push(p);
    const e = p.then(() => exec.splice(exec.indexOf(e), 1));
    exec.push(e);
    if (exec.length >= limit) await Promise.race(exec);
  }
  return Promise.allSettled(out);
}

// ===== Controller: Clean Graphs =====
exports.cleanGraphsForMCQBank = async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit || '20', 10), 100);
  const concurrency = Math.min(parseInt(req.query.concurrency || String(G_HTTP_CONCURRENCY), 10), 8);

  try {
    // Eligible rows: graph present, graphs_json null
    const { data: rows, error: fetchError } = await supabase
      .from('neet_ug_mcq_bank')
      .select('id, graph')
      .is('graphs_json', null)
      .not('graph', 'is', null)
      .order('id', { ascending: true })
      .limit(limit);

    if (fetchError) throw fetchError;
    if (!rows || rows.length === 0) {
      return res.json({
        message: 'No eligible rows found without graphs_json.',
        fetched: 0,
        updated: 0,
        failed: 0,
        model: G_MODEL
      });
    }

    const workOne = async (row) => {
      const inputText = row.graph || '';
      const prompt = `${GRAPHS_PROMPT_TEMPLATE}\n\nRaw Input:\n${inputText}`;

      try {
        const completion = await openai.chat.completions.create({
          model: G_MODEL,
          messages: [
            { role: 'system', content: 'You are a teacher generating cleaned JSON.' },
            { role: 'user', content: prompt }
          ]
        });

        const raw = completion.choices?.[0]?.message?.content ?? '';
        const parsed = gCleanAndParseJSON(raw);

        // Directly store as stringified JSON
        const { error: upErr } = await supabase
          .from('neet_ug_mcq_bank')
          .update({ graphs_json: JSON.stringify(parsed) })
          .eq('id', row.id);

        if (upErr) throw upErr;

        return { id: row.id, ok: true };
      } catch (err) {
        return { id: row.id, ok: false, error: err.message || String(err) };
      }
    };

    const results = await gAsyncPool(concurrency, rows, workOne);

    let updated = 0, failed = 0;
    const failures = [];
    for (const r of results) {
      if (r.status === 'fulfilled' && r.value?.ok) {
        updated += 1;
      } else {
        failed += 1;
        const item =
          r.status === 'fulfilled'
            ? r.value
            : { error: r.reason?.message || String(r.reason) };
        failures.push({ id: item?.id || null, error: item?.error || 'Unknown error' });
      }
    }

    return res.json({
      message: `Processed ${rows.length} Graphs.`,
      fetched: rows.length,
      updated,
      failed,
      failures: failures.slice(0, 20),
      model: G_MODEL,
      concurrency
    });
  } catch (err) {
    console.error('❌ Graphs API error:', err);
    return res.status(500).json({
      error: 'Internal Server Error',
      details: err.message || String(err)
    });
  }
};
