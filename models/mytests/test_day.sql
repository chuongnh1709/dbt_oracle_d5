{{ config(
      materialized = "view"
) }}

{% set now = modules.datetime.datetime.now().day %}
{% set now_2 = modules.datetime.datetime.today().weekday() %}

{{ log('Day of month is : ' ~ now , info=True) }}
{{ log('Function return 1 (dom): ' ~ is_today_xday()  , info=True) }}

{{ log('weekday is : ' ~ now_2 , info=True) }}
{{ log('Function return 2 (dow): ' ~ is_today_xday_of_week()  , info=True) }}

select 1 as id from dual