with source as (
    select * from {{ source('sistema_alertas', 'public_agresores') }}
),

renamed as (
    select
        id_agresor,
        nombre_agresor,
        apellido_agresor,
        url_foto_agresor
    from source
)

select * from renamed