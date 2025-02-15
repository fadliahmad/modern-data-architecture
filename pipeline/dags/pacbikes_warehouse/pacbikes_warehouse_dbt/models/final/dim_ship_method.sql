SELECT 
    {{ dbt_utils.generate_surrogate_key( ["s.shipmethodid"] ) }} as ship_method_id,
    s.shipmethodid as nk_ship_method,
    name,
    shipbase as ship_base,
    shiprate as ship_rate,
    s.modifieddate as modified_date,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
FROM {{source('pacbikes_staging', 'shipmethod')}} as s
