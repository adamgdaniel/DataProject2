with source as (
    select * from {{ source('sistema_alertas', 'alertas') }}
),

renamed as (
    select
        alerta as id_alerta, 
        activa as is_activa, 
        nivel,
        id_victima,
        id_agresor,
        distancia_metros,
        direccion_escape,
        coordenadas_agresor,
        coordenadas_victima,
        coordenadas_place,
        TIMESTAMP_TRUNC(CAST(timestamp AS TIMESTAMP), MINUTE) AS minuto_alerta,
        dist_seguridad,
        distancia_limite,
        id_place,
        nombre_place,
        radio_zona
    from source
)

select * from renamed