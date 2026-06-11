-- init_db.sql — Personne 2 : tables BDD (taux de change, API Frankfurter v2)
-- GET /rates?base=EUR&quotes=USD,GBP,JPY,CHF,CAD

CREATE SCHEMA IF NOT EXISTS fx;
SET search_path TO fx, public;


-- Personne 2 : table brute (réponse API + horodatage d'ingestion)
CREATE TABLE IF NOT EXISTS fx.raw_exchange_rates (
    raw_id            BIGSERIAL    PRIMARY KEY,
    payload           JSONB        NOT NULL,                 -- réponse brute de l'API
    base_currency     CHAR(3),                               -- devise de base (ex. EUR)
    requested_quotes  TEXT,                                  -- devises demandées
    source_endpoint   TEXT,                                  -- URL appelée
    http_status       INTEGER,                               -- code HTTP
    dag_id            TEXT,                                  -- DAG source
    run_id            TEXT,                                  -- run Airflow
    logical_date      TIMESTAMPTZ,                           -- date logique du run
    ingested_at       TIMESTAMPTZ  NOT NULL DEFAULT now()    -- horodatage d'ingestion
);

-- idempotence : un run n'insère pas deux fois la même base
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_run_base
    ON fx.raw_exchange_rates (run_id, base_currency);


-- Personne 2 : table structurée (1 ligne par paire et par date)
CREATE TABLE IF NOT EXISTS fx.exchange_rates (
    rate_id           BIGSERIAL      PRIMARY KEY,
    base_currency     CHAR(3)        NOT NULL,               -- devise de base
    quote_currency    CHAR(3)        NOT NULL,               -- devise de cotation
    rate              NUMERIC(20,10) NOT NULL,               -- taux de change
    rate_date         DATE           NOT NULL,               -- date du taux
    raw_id            BIGINT         REFERENCES fx.raw_exchange_rates(raw_id),  -- lien vers le brut
    run_id            TEXT,                                  -- run Airflow
    loaded_at         TIMESTAMPTZ    NOT NULL DEFAULT now(), -- horodatage de chargement

    CONSTRAINT uq_exchange_rate   UNIQUE (base_currency, quote_currency, rate_date),  -- idempotence
    CONSTRAINT ck_rate_positive   CHECK  (rate > 0),
    CONSTRAINT ck_diff_currencies CHECK  (base_currency <> quote_currency)
);


-- Personne 2 : cimetière (lignes rejetées par le contrôle qualité)
CREATE TABLE IF NOT EXISTS fx.rejected_exchange_rates (
    rejection_id      BIGSERIAL    PRIMARY KEY,
    base_currency     TEXT,                                  -- valeur reçue (peut être invalide)
    quote_currency    TEXT,                                  -- valeur reçue
    rate              TEXT,                                  -- valeur reçue
    rate_date         TEXT,                                  -- valeur reçue
    quality_dimension TEXT         NOT NULL,                 -- completude|coherence|fraicheur|unicite|structure
    rejection_reason  TEXT         NOT NULL,                 -- motif du rejet
    raw_record        JSONB,                                 -- enregistrement source
    raw_id            BIGINT       REFERENCES fx.raw_exchange_rates(raw_id),
    run_id            TEXT,                                  -- run Airflow
    rejected_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),   -- horodatage du rejet

    CONSTRAINT ck_quality_dimension CHECK (
        quality_dimension IN ('completude','coherence','fraicheur','unicite','structure')
    )
);
