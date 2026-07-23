-- Flags markets whose price for a commodity looks unusual compared to OTHER
-- markets selling the same commodity, in the same state, on the same day.
--
-- Method: IQR (interquartile range). For each (state, commodity, arrival_date)
-- group, compute Q1 (25th percentile) and Q3 (75th percentile) of modal_price.
-- Anything outside [Q1 - 1.5*IQR, Q3 + 1.5*IQR] is flagged as an anomaly -
-- a standard, well-understood outlier-detection rule (not something invented
-- for this project - same rule boxplots use to draw their "whiskers").
--
-- We compare WITHIN a state, not across all of India, because freight cost,
-- local demand, and regional growing patterns make cross-state price
-- differences normal and not evidence of anything unusual.
--
-- We only flag groups with at least 3 markets reporting - with 1-2 markets,
-- percentiles aren't meaningful (e.g. with 2 markets, Q1 and Q3 just ARE
-- the two prices, so nothing can ever look "outside the range").

with facts as (

    select * from {{ ref('fct_mandi_prices') }}

),

with_group_stats as (

    select
        *,
        quantile_cont(modal_price, 0.25)
            over (partition by state, commodity, arrival_date) as q1,
        quantile_cont(modal_price, 0.75)
            over (partition by state, commodity, arrival_date) as q3,
        count(*)
            over (partition by state, commodity, arrival_date) as n_markets_reporting,
        median(modal_price)
            over (partition by state, commodity, arrival_date) as group_median_price

    from facts

),

with_bounds as (

    select
        *,
        (q3 - q1) as iqr,
        q1 - 1.5 * (q3 - q1) as lower_bound,
        q3 + 1.5 * (q3 - q1) as upper_bound

    from with_group_stats

)

select
    state,
    district,
    market,
    commodity,
    variety,
    arrival_date,
    modal_price,
    group_median_price,
    n_markets_reporting,
    round(lower_bound, 2) as expected_lower_bound,
    round(upper_bound, 2) as expected_upper_bound,
    round(
        100.0 * (modal_price - group_median_price) / nullif(group_median_price, 0),
        1
    ) as pct_deviation_from_state_median,
    case
        when n_markets_reporting >= 3
             and group_median_price >= 50  -- guard against tiny-denominator % blowups
                                            -- (e.g. a likely unit-mismatch data
                                            -- error, not a real price gap)
             and (modal_price < lower_bound or modal_price > upper_bound)
        then true
        else false
    end as is_price_anomaly

from with_bounds
where n_markets_reporting >= 3  -- can't meaningfully flag anomalies with fewer than 3 markets to compare