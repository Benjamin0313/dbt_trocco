{{ config(schema="dwh", materialized="table") }}

with products as (
    select * from {{ source("jaffle_shop", "PRODUCTS") }}
)

, cast_type_products as (
    select
        products.sku as sku,
        products.name as name,
        products.type as type,
        products.price as price,
        products.description as description,
    from products
),
final as (
    select
        cast_type_products.sku,
        cast_type_products.name,
        cast_type_products.type,
        cast(cast_type_products.price as float64) as price,
        cast_type_products.description,
    from cast_type_products
)
select * from final