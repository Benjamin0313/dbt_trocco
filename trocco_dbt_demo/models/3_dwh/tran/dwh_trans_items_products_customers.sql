{{ config(schema="dwh", materialized="table") }}

with
    /* import ctes*/
    trans_buy_items as (select * from {{ ref("dwh_trans_buy_items") }}),
    master_customers as (select * from {{ ref("dwh_master_customers") }}),
    master_products as (select * from {{ ref("dwh_master_products") }}),
    join_items_customers_products as (
        select
            trans_buy_items.buy_items_id,
            trans_buy_items.sku,
            trans_buy_items.customer_id,
            trans_buy_items.subtotal,
            trans_buy_items.tax_paid,
            trans_buy_items.order_total,
            trans_buy_items.ordered_date,
            trans_buy_items.`曜日コード`,
            trans_buy_items.`曜日`,
            trans_buy_items.`土日祝`,
            trans_buy_items.`祝日名`,
            trans_buy_items.`GW`,
            trans_buy_items.`GW_中間も休み`,
            trans_buy_items.`年末年始`,
            trans_buy_items.`連休`,
            trans_buy_items.`観測所名`,
            trans_buy_items.`平均気温`,
            trans_buy_items.`平均湿度`,
            trans_buy_items.`天気概況_昼`,
            trans_buy_items.`天気概況_夜`,
            master_customers.gender,
            master_customers.age_group,
            master_customers.email,
            master_products.name,
        from trans_buy_items
        left join
            master_customers
            on trans_buy_items.customer_id = master_customers.customer_id
        left join master_products on trans_buy_items.sku = master_products.sku
    ),
    final as (
        select
            join_items_customers_products.buy_items_id,
            join_items_customers_products.sku,
            join_items_customers_products.customer_id,
            join_items_customers_products.subtotal,
            join_items_customers_products.tax_paid,
            join_items_customers_products.order_total,
            join_items_customers_products.ordered_date,
            join_items_customers_products.`曜日コード`,
            join_items_customers_products.`曜日`,
            join_items_customers_products.`土日祝`,
            join_items_customers_products.`祝日名`,
            join_items_customers_products.`GW`,
            join_items_customers_products.`GW_中間も休み`,
            join_items_customers_products.`年末年始`,
            join_items_customers_products.`連休`,
            join_items_customers_products.`観測所名`,
            join_items_customers_products.`平均気温`,
            join_items_customers_products.`平均湿度`,
            join_items_customers_products.`天気概況_昼`,
            join_items_customers_products.`天気概況_夜`,
            join_items_customers_products.gender,
            join_items_customers_products.age_group,
            join_items_customers_products.email,
            join_items_customers_products.name as product_name,
        from join_items_customers_products
    )
select *
from final