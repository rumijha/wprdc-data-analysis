CREATE SCHEMA IF NOT EXISTS output;

CREATE OR REPLACE TABLE output.municipality_year_sales AS
WITH annual AS (
    SELECT municipality_key, municipality, sale_year,
        COUNT(*) AS sale_count,
        MEDIAN(price::DECIMAL(19, 3)) AS median_sale_price
    FROM cleaned.property_sales
    WHERE is_market_sale AND sale_year BETWEEN $start_year AND $end_year
    GROUP BY municipality_key, municipality, sale_year
), changes AS (
    SELECT current.*,
        current.sale_count < $min_sample AS low_sample_flag,
        previous.sale_count AS previous_year_sale_count,
        previous.median_sale_price AS previous_year_median_sale_price,
        current.median_sale_price - previous.median_sale_price AS yoy_price_change,
        100.0 * (current.median_sale_price - previous.median_sale_price)
            / NULLIF(previous.median_sale_price, 0) AS yoy_percent_change,
        COALESCE(
            current.sale_count >= $min_sample
            AND previous.sale_count >= $min_sample
            AND previous.median_sale_price > 0, FALSE
        ) AS ranking_eligible
    FROM annual AS current
    LEFT JOIN annual AS previous
        ON current.municipality_key = previous.municipality_key
        AND current.sale_year = previous.sale_year + 1
), ranked AS (
    SELECT municipality_key, sale_year,
        DENSE_RANK() OVER (
            PARTITION BY sale_year ORDER BY yoy_percent_change DESC
        ) AS yoy_growth_rank
    FROM changes
    WHERE ranking_eligible
)
SELECT changes.*, ranked.yoy_growth_rank
FROM changes
LEFT JOIN ranked USING (municipality_key, sale_year);