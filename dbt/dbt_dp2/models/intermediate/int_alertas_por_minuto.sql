WITH alertas_limpias AS (
    SELECT * FROM {{ ref('stg_alertas') }}
)

SELECT 
    minuto_alerta,
    id_victima,
    id_agresor,
    id_place,
    MAX(nombre_place) AS nombre_place,
    COUNT(*) AS cantidad_pings_en_minuto,
    MIN(distancia_metros) AS distancia_minima_alcanzada,
    MAX(distancia_limite) AS distancia_limite,
    MAX(nivel) AS nivel_maximo_minuto,
    MAX(direccion_escape) AS direccion_escape_minuto

FROM alertas_limpias
GROUP BY 
    minuto_alerta,
    id_victima,
    id_agresor,
    id_place