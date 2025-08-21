-- Таблица событий скоринга
CREATE TABLE IF NOT EXISTS public.score_events (
  id BIGSERIAL PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  score DOUBLE PRECISION NOT NULL,
  fraud_flag BOOLEAN NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Индексы под типичные запросы
CREATE INDEX IF NOT EXISTS idx_score_events_tid        ON public.score_events (transaction_id);
CREATE INDEX IF NOT EXISTS idx_score_events_created_at ON public.score_events (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_score_events_fraud      ON public.score_events (fraud_flag, created_at DESC);

-- Витрина (все события)
CREATE OR REPLACE VIEW public.transaction_scores AS
SELECT
  transaction_id,
  score AS model_score,
  fraud_flag,
  created_at
FROM public.score_events;

-- Витрина «последний скор на транзакцию»
CREATE OR REPLACE VIEW public.transaction_scores_latest AS
SELECT DISTINCT ON (transaction_id)
  transaction_id,
  score AS model_score,
  fraud_flag,
  created_at
FROM public.score_events
ORDER BY transaction_id, created_at DESC;
