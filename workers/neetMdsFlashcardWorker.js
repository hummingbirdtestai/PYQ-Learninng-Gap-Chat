SELECT
  COUNT(*) AS total_rows,

  COUNT(*) FILTER (
    WHERE jsonb_typeof(pyq_content -> 'pyqs') = 'array'
      AND jsonb_array_length(pyq_content -> 'pyqs') > 0
  ) AS rows_with_pyqs,

  COUNT(*) FILTER (
    WHERE jsonb_output IS NOT NULL
  ) AS flashcards_completed,

  COUNT(*) FILTER (
    WHERE jsonb_output IS NULL
      AND generation_lock = false
      AND jsonb_typeof(pyq_content -> 'pyqs') = 'array'
      AND jsonb_array_length(pyq_content -> 'pyqs') > 0
  ) AS flashcards_pending,

  COUNT(*) FILTER (
    WHERE jsonb_output IS NULL
      AND generation_lock = true
  ) AS flashcards_processing,

  COUNT(*) FILTER (
    WHERE jsonb_typeof(pyq_content -> 'pyqs') <> 'array'
       OR jsonb_array_length(
            CASE
              WHEN jsonb_typeof(pyq_content -> 'pyqs') = 'array'
              THEN pyq_content -> 'pyqs'
              ELSE '[]'::jsonb
            END
          ) = 0
  ) AS rows_without_pyqs

FROM public.neet_mds_pyt_source;
