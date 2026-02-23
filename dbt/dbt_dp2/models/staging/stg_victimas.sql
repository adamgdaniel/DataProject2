with source as (
    select * from {{ source('sistema_alertas', 'public_victimas') }}
),

renamed as (
    select
        id_victima,
        nombre_victima,
        apellido_victima,
        url_foto_victima
    from source
)

select * from renamed