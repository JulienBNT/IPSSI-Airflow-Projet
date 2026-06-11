SET search_path TO fx, public;

-- KPI 1: Tendance recente et variation quotidienne
-- moyenne mobile 7 jours + variation quotidienne
-- question metier: quelle est la tendance recente d'une paire
-- et quel est son ecart par rapport a la veille
-- graphique en ligne par paire de devises

CREATE OR REPLACE VIEW fx.vw_taux_moyenne_mobile AS
SELECT
    base_currency,
    quote_currency,
    rate_date,
    rate,

    ROUND(
        AVG(rate) OVER (
            PARTITION BY base_currency, quote_currency
            ORDER BY rate_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )::NUMERIC, 6
    ) AS moyenne_7j,

    ROUND(
        AVG(rate) OVER (
            PARTITION BY base_currency, quote_currency
            ORDER BY rate_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )::NUMERIC, 6
    ) AS moyenne_30j,

    ROUND((
        (rate - LAG(rate) OVER (
            PARTITION BY base_currency, quote_currency
            ORDER BY rate_date
        ))
        / NULLIF(LAG(rate) OVER (
            PARTITION BY base_currency, quote_currency
            ORDER BY rate_date
        ), 0) * 100
    )::NUMERIC, 4) AS variation_j_pct

FROM fx.exchange_rates
ORDER BY base_currency, quote_currency, rate_date DESC;


-- KPI 2: Volatilite et amplitude historique par paire
-- question metier: quelles paires de devises sont les plus
-- risquees / stables sur la periode chargee 
-- tableau classe par volatilite
CREATE OR REPLACE VIEW fx.vw_volatilite_paires AS
SELECT
    base_currency,
    quote_currency,
    COUNT(*)                                                    AS nb_observations,
    MIN(rate_date)                                              AS premiere_date,
    MAX(rate_date)                                              AS derniere_date,
    ROUND(AVG(rate)::NUMERIC,    6)                            AS taux_moyen,
    ROUND(STDDEV(rate)::NUMERIC, 6)                            AS ecart_type,
    ROUND(MIN(rate)::NUMERIC,    6)                            AS taux_min,
    ROUND(MAX(rate)::NUMERIC,    6)                            AS taux_max,
    ROUND(
        ((MAX(rate) - MIN(rate)) / NULLIF(MIN(rate), 0) * 100)::NUMERIC, 4
    )                                                           AS amplitude_pct,

    -- 1 = paire la plus volatile
    RANK() OVER (ORDER BY STDDEV(rate) DESC)                   AS rang_volatilite

FROM fx.exchange_rates
GROUP BY base_currency, quote_currency
ORDER BY ecart_type DESC;
