with source as (
    select * from {{ source('sistema_alertas', 'public_safe_places') }}
),

renamed as (
    select
        id_place,
        place_coordinates as coordenadas_place, -- Unificamos el idioma si prefieres
        radius as radio_metros,
        place_name as nombre_place
    from source
)

select * from renamed