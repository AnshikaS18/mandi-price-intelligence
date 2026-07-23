-- Analysis-ready fact table: one row per (market, commodity, variety, arrival_date).
-- If you've run ingestion more than once on the same day, this keeps only the
-- MOST RECENT ingestion run per natural key, so re-running ingestion is safe
-- and doesn't create duplicate rows here.

with staged as (

    select * from {{ ref('stg_mandi_prices') }}

),

ranked as (

    select
        *,
        row_number() over (
            partition by state, district, market, commodity, variety, grade, arrival_date
            order by ingestion_run_id desc
        ) as row_num

    from staged
    where modal_price is not null  -- drop rows we couldn't parse a price for

)

select
    state,
    district,
    market,
    commodity,
    variety,
    grade,
    arrival_date,
    min_price,
    max_price,
    modal_price
from ranked
where row_num = 1
