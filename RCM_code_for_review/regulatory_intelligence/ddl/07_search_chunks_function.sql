/* =====================================================================
   07_search_chunks_function.sql
   search_chunks: the search tool used by the Q and A Agent.
   Current version: searches rcm_chunk_embedding with SQL (meaning plus keywords, no idle cost).
   With Databricks Vector Search, only the body of this function changes.
   The inputs and the output columns must stay the same, so the agent does not change.
   ===================================================================== */

CREATE OR REPLACE FUNCTION ${catalog}.${schema}.search_chunks(
    question   STRING COMMENT 'The user question in plain English',
    regulator  STRING DEFAULT NULL COMMENT 'Regulator id such as MAS, or empty to search all regulators',
    top_k      INT    DEFAULT 5    COMMENT 'How many chunks to return'
)
RETURNS TABLE (
    chunk_id        STRING,
    document_id     STRING,
    version         INT,
    regulator_id    STRING,
    section_title   STRING,
    published_date  DATE,
    source_url      STRING,
    chunk_text      STRING,
    score           DOUBLE
)
COMMENT 'Finds the regulatory text chunks that best match a question. Combines meaning (embeddings) and exact words (keywords).'
RETURN
    -- Step 1: turn the question into 1024 numbers with the same model used for the chunks
    WITH q AS (
        SELECT ai_query('${embedding_model}', search_chunks.question) AS v
    ),
    -- Step 2: the important words of the question, used for the keyword score (for example 656, fhc, n652)
    words AS (
        SELECT filter(
                   split(lower(search_chunks.question), '[^a-z0-9]+'),
                   w -> length(w) >= 3 AND NOT array_contains(
                       array('the','and','for','what','about','does','did','say','says','with','from',
                             'that','this','which','are','was','how','when','there','any','new','changes'), w)
               ) AS w
    ),
    -- Step 3: score every chunk
    scored AS (
        SELECT e.chunk_id, e.document_id, e.version, e.regulator_id, e.section_title,
               e.published_date, e.source_url, e.chunk_text,
               -- meaning score: cosine similarity between the question and the chunk (higher is closer)
               aggregate(zip_with(e.embedding, q.v, (a, b) -> a * b), 0D, (acc, x) -> acc + x)
                 / (sqrt(aggregate(e.embedding, 0D, (acc, x) -> acc + x * x))
                    * sqrt(aggregate(q.v, 0D, (acc, x) -> acc + x * x)))           AS meaning_score,
               -- keyword score: share of the important words that appear in the chunk (0 to 1)
               size(filter(words.w, k -> instr(lower(e.chunk_text), k) > 0))
                 / greatest(size(words.w), 1)                                        AS keyword_score
        FROM ${catalog}.${schema}.rcm_chunk_embedding e
        CROSS JOIN q
        CROSS JOIN words
        WHERE search_chunks.regulator IS NULL OR e.regulator_id = search_chunks.regulator
    ),
    -- Step 4: combine the two scores and rank the chunks
    ranked AS (
        SELECT *,
               0.8 * meaning_score + 0.2 * keyword_score                             AS score,
               row_number() OVER (ORDER BY 0.8 * meaning_score + 0.2 * keyword_score DESC) AS rn
        FROM scored
    )
    -- Step 5: return the best chunks
    SELECT chunk_id, document_id, version, regulator_id, section_title,
           published_date, source_url, chunk_text, score
    FROM ranked
    WHERE rn <= search_chunks.top_k;
