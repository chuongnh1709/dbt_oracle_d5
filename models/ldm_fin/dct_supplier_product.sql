{{ config(materialized='table', sort='timestamp', dist='user_id', tags='dct_supplier_product') }}

-- Local variables
{% set v_heavy = 'heavy' %}
{% set v_small_weight = 'small_weight' %}
{% set v_mid_weight = 'mid_weight' %}

-- Join model  with CTE and  Ref Function 
with product as
  (
    select 
        product.supplier_id                                                 as supplier_id
      , product.product_id                                                  as product_id
      , product.product_name                                                as product_name
      , coalesce(product.brand_name, '{{ var("v_xna") }}')                  as brand_name
      , coalesce(product.product_size , '{{ var("v_xna") }}' )              as product_size
      , product.typical_weight_per_unit                                     as typical_weight_per_unit
      , case 
          when product.typical_weight_per_unit < 5              then 'small_weight'
          when product.typical_weight_per_unit between 5 and 10 then 'mid_weight'
          else '{{v_heavy}}'
          end                                                   as weight_type
    from {{ ref('dct_product') }}  as product
  )
select 
    generate_uuid()                                             as skp_supplier_product  -- Bigquery only
  , product.product_id ||'.' ||supplier.supplier_id             as id_source
  , product.product_id                                          as product_id
  , product.product_name                                        as product_name
  , product.brand_name                                          as brand_name
  , product.brand_name                                          as product_size
  , product.typical_weight_per_unit                             as typical_weight_per_unit
  , product.weight_type                                         as weight_type
  , supplier.supplier_id                                        as supplier_id
  , supplier.supplier_category_id                               as supplier_category_id
  , supplier.supplier_name                                      as supplier_name 
  , coalesce(supplier.phone_number, '{{ var("v_xna") }}' )      as phone_number
from product 
join {{ ref('dct_supplier') }} supplier
  on product.supplier_id = supplier.supplier_id

/* Bigquery sql 
select 
    product.stock_item_id ||'.' ||supplier.supplier_id  as id_source
  , product.stock_item_id                               as product_id
  , product.stock_item_name                             as product_name 
  , coalesce(product.brand, 'xna')                      as brand_name
  , coalesce(product.size , 'xna')                      as product_size
  , product.typical_weight_per_unit                     as typical_weight_per_unit
  , case 
      when product.typical_weight_per_unit < 5  then 'small_weight'
      when product.typical_weight_per_unit between 5 and 10 then 'mid_weight'
      else 'heavy'
      end                                               as weight_type
  , supplier.supplier_id                                as supplier_id
  , supplier.supplier_category_id                       as supplier_category_id
  , upper(supplier.supplier_name)                       as supplier_name 
from `vit-lam-data.wide_world_importers.warehouse__stock_items` as product
join `vit-lam-data.wide_world_importers.purchasing__suppliers`  as supplier
  on product.supplier_id = supplier.supplier_id
*/