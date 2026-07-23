-- Staging model: one clean, typed row per raw price record.
-- Reads directly from every CSV the ingestion script has dropped in data/raw/,
-- so re-running `dbt run` after a new daily pull picks up new files automatically.

with source as (

    select *
    from read_csv_auto(
        '../data/raw/mandi_prices_*.csv',
        union_by_name = true,
        all_varchar = true,  -- read everything as text first; we cast explicitly below.
                             -- Govt data is messy - safer to control casting ourselves
                             -- than let DuckDB guess and silently drop bad rows.
        filename = true      -- adds a `filename` column - lets us tell which
                             -- ingestion run each row came from
    )

),

cleaned as (

    select
        trim(state)                                as state,
        trim(district)                              as district,
        trim(market)                                as market,
        trim(commodity)                             as commodity,
        trim(variety)                                as variety,
        trim(grade)                                  as grade,

        -- arrival_date arrives as DD/MM/YYYY text; convert to a real date
        strptime(trim(arrival_date), '%d/%m/%Y')::date as arrival_date,

        try_cast(min_price as double)               as min_price,
        try_cast(max_price as double)                as max_price,
        try_cast(modal_price as double)              as modal_price,

        -- pull the run timestamp out of the filename, e.g.
        -- 'mandi_prices_20260723_112947.csv' -> 20260723_112947
        -- used downstream to keep only the LATEST ingestion run per day
        regexp_extract(filename, 'mandi_prices_(\d{8}_\d{6})', 1) as ingestion_run_id

    from source

)

select * from cleaned
