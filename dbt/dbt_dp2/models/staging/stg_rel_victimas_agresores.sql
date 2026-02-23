with source as (
    select * from {{ source('sistema_alertas', 'public_rel_victimas_agresores') }}
),

renamed as (
    select
        id_agresor,
        id_victima,
        dist_seguridad
    from source
)

select * from renamed