{{ config(schema="dwh", materialized="table") }}

with
    /* import ctes*/
    items as (select * from {{ source("jaffle_shop", "ITEMS") }}),
    orders as (select * from {{ source("jaffle_shop", "ORDERS") }}),
    podb_weather_data as (
        /* jaffle_shopは東京都内にある1店舗と仮定する、観測所は東京*/
        select * from {{ source("podb_weather_data", "WT_MD") }} where observatory_name = '東京'
    ),
    podb_calendar_data as (
        select * from {{ source("podb_calendar_data", "JAPAN_CALENDAR") }}
    ),

    join_items_orders as (
        select
            items.id as buy_items_id,
            items.order_id,
            items.sku,
            orders.customer as customer_id,
            /* 時間は不要なのでここで落とす*/
            date(orders.ordered_at) as ordered_date,
            orders.subtotal,
            orders.tax_paid,
            orders.order_total
        from items
        left join orders on items.order_id = orders.id
    ),
    join_items_weather as (
        select
            join_items_orders.buy_items_id,
            join_items_orders.order_id,
            join_items_orders.sku,
            join_items_orders.customer_id,
            join_items_orders.ordered_date,
            join_items_orders.subtotal,
            join_items_orders.tax_paid,
            join_items_orders.order_total,
            podb_weather_data.observatory_name as `観測所名`,
            podb_weather_data.air_temperature as `平均気温`,
            podb_weather_data.humidity as `平均湿度`,
            podb_weather_data.weather_daytime as `天気概況_昼`,
            podb_weather_data.weather_nighttime as `天気概況_夜`
        from join_items_orders
        left join
            podb_weather_data on join_items_orders.ordered_date = podb_weather_data.date
    ),
    join_items_calendar as (
        select
            join_items_weather.buy_items_id,
            join_items_weather.order_id,
            join_items_weather.sku,
            join_items_weather.customer_id,
            join_items_weather.subtotal,
            join_items_weather.tax_paid,
            join_items_weather.order_total,
            join_items_weather.ordered_date,
            podb_calendar_data.weekday_code as `曜日コード`,
            podb_calendar_data.weekday as  `曜日`,
            podb_calendar_data.is_sat_sun_hol as `土日祝`,
            podb_calendar_data.holiday_name as `祝日名`,
            podb_calendar_data.is_gw as `GW`,
            podb_calendar_data.is_gw_long as `GW_中間も休み`,
            podb_calendar_data.is_year_end_and_new_year_hol as `年末年始`,
            podb_calendar_data.is_consecutive_days_off as `連休`,
            join_items_weather.`観測所名`,
            join_items_weather.`平均気温`,
            join_items_weather.`平均湿度`,
            join_items_weather.`天気概況_昼`,
            join_items_weather.`天気概況_夜`
        from join_items_weather
        left join
            podb_calendar_data
            on join_items_weather.ordered_date = podb_calendar_data.date
    ),
    cast_type_items as (
        select
            cast(join_items_calendar.buy_items_id as string) as buy_items_id,
            cast(join_items_calendar.order_id as string) as order_id,
            cast(join_items_calendar.sku as string) as sku,
            cast(join_items_calendar.customer_id as string) as customer_id,
            cast(join_items_calendar.subtotal as int64) as subtotal,
            cast(join_items_calendar.tax_paid as int64) as tax_paid,
            cast(join_items_calendar.order_total as int64) as order_total,
            cast(join_items_calendar.ordered_date as date) as ordered_date,
            cast(join_items_calendar.`曜日コード` as string) as `曜日コード`,
            cast(join_items_calendar.`曜日` as string) as `曜日`,
            cast(join_items_calendar.`土日祝` as boolean) as `土日祝`,
            cast(join_items_calendar.`祝日名` as string) as `祝日名`,
            cast(join_items_calendar.`GW` as boolean) as `GW`,
            cast(join_items_calendar.`GW_中間も休み` as boolean) as `GW_中間も休み`,
            cast(join_items_calendar.`年末年始` as boolean) as `年末年始`,
            cast(join_items_calendar.`連休` as boolean) as `連休`,
            cast(join_items_calendar.`観測所名` as string) as `観測所名`,
            cast(join_items_calendar.`平均気温` as int64) as `平均気温`,
            cast(join_items_calendar.`平均湿度` as int64) as `平均湿度`,
            cast(join_items_calendar.`天気概況_昼` as string) as `天気概況_昼`,
            cast(join_items_calendar.`天気概況_夜` as string) as `天気概況_夜`
        from join_items_calendar

    ),
    final as (
        select
            cast_type_items.buy_items_id,
            cast_type_items.order_id,
            cast_type_items.sku,
            cast_type_items.customer_id,
            cast_type_items.subtotal,
            cast_type_items.tax_paid,
            cast_type_items.order_total,
            cast_type_items.ordered_date,
            cast_type_items.`曜日コード`,
            cast_type_items.`曜日`,
            cast_type_items.`土日祝`,
            cast_type_items.`祝日名`,
            cast_type_items.`GW`,
            cast_type_items.`GW_中間も休み`,
            cast_type_items.`年末年始`,
            cast_type_items.`連休`,
            cast_type_items.`観測所名`,
            cast_type_items.`平均気温`,
            cast_type_items.`平均湿度`,
            cast_type_items.`天気概況_昼`,
            cast_type_items.`天気概況_夜`
        from cast_type_items
    )
select *
from final