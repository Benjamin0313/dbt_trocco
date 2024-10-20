{{ config(schema="lake", materialized="table", tags=["lake", "jaffle_shop", "js"]) }}

-- テーブルの構造に合わせて顧客情報を生成
-- 顧客情報を一致させるため、rand_seedは42とする。
with customers as (
    select * from {{ source('jaffle_shop', 'CUSTOMERS') }}
)

, generated_customer_features as (
    select
        customers.id as customer_id,
        -- 性別をランダムに生成 (NULLも考慮)
        case
            when rand() < 0.33
            then '男'
            when rand() < 0.66
            then '女'
            when rand() < 0.99
            then 'その他'
            else null  -- NULL値
        end as gender,

        -- 年齢をランダムに生成、20%の確率でNULLにする
        case
            WHEN RAND() < 0.2 THEN NULL 
            ELSE FLOOR(10 + RAND() * (110 - 10))  -- 10歳以上110歳未満
        end as age,

        -- ランダムなemailアドレス生成、15%の確率でNULL
        CASE
        WHEN RAND() < 0.15 THEN NULL
        ELSE (
            SELECT LOWER(STRING_AGG(CHR(CAST(FLOOR(97 + RAND() * 26) AS INT64)), ''))
            FROM UNNEST(GENERATE_ARRAY(1, 10)) -- 10文字のランダムな英小文字
        ) || '@example.com'
        END as email
    from customers
)

, transfer_type as (
    select
        generated_customer_features.customer_id as customer_id,
        generated_customer_features.gender as gender,
        generated_customer_features.age as age,
        generated_customer_features.email as email
    from generated_customer_features
)

, final as (
    select
        transfer_type.customer_id,
        transfer_type.gender,
        transfer_type.age,
        transfer_type.email
    from transfer_type
)

select * from final