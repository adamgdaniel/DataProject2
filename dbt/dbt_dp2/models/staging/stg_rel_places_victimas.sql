with source as (
    select * from {{ source('sistema_alertas', 'public_rel_places_victimas') }}
),

renamed as (
    select
        id_victima,
        id_place
    from source
)

select * from renamed