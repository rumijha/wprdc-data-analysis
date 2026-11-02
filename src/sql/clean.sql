CREATE SCHEMA IF NOT EXISTS cleaned;

CREATE OR REPLACE TABLE cleaned.property_sales AS
WITH normalized AS (
    SELECT
        NULLIF(TRIM(_id), '') AS source_id,
        NULLIF(TRIM(PARID), '') AS parcel_id,
        NULLIF(TRIM(FULL_ADDRESS), '') AS full_address,
        NULLIF(TRIM(MUNICODE), '') AS municipality_code,
        REPLACE(NULLIF(TRIM(MUNIDESC), ''), '''''', '''') AS municipality_description,
        NULLIF(TRIM(SALECODE), '') AS sale_code,
        NULLIF(TRIM(SALEDATE), '') AS sale_date_text,
        NULLIF(TRIM(RECORDDATE), '') AS record_date_text,
        NULLIF(TRIM(PRICE), '') AS price_text,
        DEEDBOOK AS source_deed_book,
        DEEDPAGE AS source_deed_page,
        CASE WHEN TRIM(DEEDBOOK) IN ('', '0', '00', '000') THEN NULL
             ELSE TRIM(DEEDBOOK) END AS deed_book,
        CASE WHEN TRIM(DEEDPAGE) IN ('', '0', '00', '000') THEN NULL
             ELSE TRIM(DEEDPAGE) END AS deed_page
    FROM raw.property_sales
), parsed AS (
    SELECT *,
        TRY_CAST(sale_date_text AS DATE) AS sale_date,
        TRY_CAST(record_date_text AS DATE) AS parsed_record_date,
        TRY_CAST(price_text AS DECIMAL(18, 2)) AS price,
        NOT COALESCE(
            REGEXP_FULL_MATCH(sale_date_text, '[0-9]{4}-[0-9]{2}-[0-9]{2}')
            AND TRY_CAST(sale_date_text AS DATE) IS NOT NULL, FALSE
        ) AS sale_date_parse_invalid,
        record_date_text IS NOT NULL AND NOT COALESCE(
            REGEXP_FULL_MATCH(record_date_text, '[0-9]{4}-[0-9]{2}-[0-9]{2}')
            AND TRY_CAST(record_date_text AS DATE) IS NOT NULL, FALSE
        ) AS record_date_parse_invalid,
        price_text IS NOT NULL AND (
            NOT REGEXP_FULL_MATCH(price_text, '-?[0-9]+(\.[0-9]{1,2})?')
            OR TRY_CAST(price_text AS DECIMAL(18, 2)) IS NULL
        ) AS price_parse_invalid,
        deed_book IS NULL OR deed_page IS NULL AS deed_reference_missing
    FROM normalized
), derived AS (
    SELECT *,
        COALESCE(parsed_record_date < DATE '2000-01-01', FALSE) AS record_date_invalid,
        CASE WHEN parsed_record_date >= DATE '2000-01-01'
             THEN parsed_record_date END AS record_date,
        YEAR(sale_date)::INTEGER AS sale_year,
        CASE
            WHEN municipality_code IN (SELECT RANGE::VARCHAR FROM RANGE(101, 133)) THEN 'city:pittsburgh'
            WHEN municipality_code IN ('201', '202') THEN 'city:clairton'
            WHEN municipality_code IN ('301', '302', '303') THEN 'city:duquesne'
            WHEN municipality_code IN (SELECT RANGE::VARCHAR FROM RANGE(401, 413)) THEN 'city:mckeesport'
            ELSE 'municipality:' || municipality_code
        END AS municipality_key,
        CASE
            WHEN municipality_code IN (SELECT RANGE::VARCHAR FROM RANGE(101, 133)) THEN 'Pittsburgh'
            WHEN municipality_code IN ('201', '202') THEN 'Clairton'
            WHEN municipality_code IN ('301', '302', '303') THEN 'Duquesne'
            WHEN municipality_code IN (SELECT RANGE::VARCHAR FROM RANGE(401, 413)) THEN 'McKeesport'
            ELSE municipality_description
        END AS municipality,
        CASE WHEN NOT deed_reference_missing
             THEN COUNT(*) OVER (PARTITION BY deed_book, deed_page)
        END AS parcels_on_deed
    FROM parsed
)
SELECT * EXCLUDE (parsed_record_date),
    COALESCE(
        sale_code = '0' AND price >= 1000
        AND (parcels_on_deed = 1 OR deed_reference_missing), FALSE
    ) AS is_market_sale,
    CASE
        WHEN sale_code IS DISTINCT FROM '0' THEN 'not_valid_sale_code'
        WHEN price IS NULL THEN 'missing_price'
        WHEN price < 1000 THEN 'below_price_floor'
        WHEN parcels_on_deed > 1 THEN 'multi_parcel_deed'
        ELSE NULL
    END AS exclusion_reason
FROM derived;