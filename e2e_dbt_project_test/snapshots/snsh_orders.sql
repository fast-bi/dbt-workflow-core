{% snapshot snsh_orders %}

{{
    config(
      target_schema='e2e_image_test_jaffle_shop_data_snapshot',
      unique_key='order_id',
      strategy='timestamp',
      updated_at='order_date',
    )
}}

select * from {{ ref('orders') }}

{% endsnapshot %}