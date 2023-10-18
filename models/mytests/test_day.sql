{{ config(
      materialized = "view"
) }}

{% set now = modules.datetime.datetime.now().day %}
{% set now_2 = modules.datetime.datetime.today().weekday() %}

{{ log('Day of month is : ' ~ now , info=True) }}
{{ log('Function return 1 : ' ~ is_today_xday()  , info=True) }}

{{ log('now_2 is : ' ~ now_2 , info=True) }}
{{ log('Function return 2 : ' ~ is_today_xday_of_week()  , info=True) }}

select 1 as id from dual