WITH alertas AS (
    SELECT * FROM {{ ref('int_alertas_por_minuto') }}
)

SELECT 
    minuto_alerta,
    id_victima,
    id_agresor,
    id_place,
    distancia_limite,
    distancia_minima_alcanzada,
    nivel_maximo_minuto,
    
    GREATEST(
        0, 
        ROUND(SAFE_DIVIDE((distancia_limite - distancia_minima_alcanzada), distancia_limite) * 100, 2)
    ) AS porcentaje_gravedad

FROM alertas