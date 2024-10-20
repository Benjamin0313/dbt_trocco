{{ config(schema="dwh", materialized="table") }}

with customer_features as (
    select * from {{ ref("lake_customer_features") }}
)
, age_buckets as (
    select * from {{ ref('age_buckets') }}
)
, make_age_buckets as (
    select
        customer_features.customer_id,
        customer_features.gender,
        customer_features.age,
        customer_features.email,
        age_buckets.age_group,
    from customer_features
    left join
        age_buckets
        on customer_features.age
        between age_buckets.age_min and age_buckets.age_max
)
, final as (
    select
        make_age_buckets.customer_id,
        make_age_buckets.gender,
        make_age_buckets.email,
        make_age_buckets.age_group,
        cast(make_age_buckets.age as int64) as age,
    from make_age_buckets
)
select *from final