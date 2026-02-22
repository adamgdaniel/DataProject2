WITH alertas_safe_places AS (
    SELECT a.* FROM {{ ref('int_alertas_por_minuto') }} a
    INNER JOIN {{ ref('stg_rel_places_victimas') }} r 
        ON a.id_place = r.id_place AND a.id_victima = r.id_victima
),

diferencias_tiempo AS (
    SELECT 
        *,
        TIMESTAMP_DIFF(
            minuto_alerta, 
            LAG(minuto_alerta) OVER (PARTITION BY id_agresor, id_victima, id_place ORDER BY minuto_alerta), 
            MINUTE
        ) AS mins_desde_ultima_alerta
    FROM alertas_safe_places
),

marcas_nuevo_asedio AS (
    SELECT 
        *,
        IF(mins_desde_ultima_alerta IS NULL OR mins_desde_ultima_alerta > 15, 1, 0) AS nuevo_asedio_flag
    FROM diferencias_tiempo
),

sesiones_asedio AS (
    SELECT 
        *,
        SUM(nuevo_asedio_flag) OVER (PARTITION BY id_agresor, id_victima, id_place ORDER BY minuto_alerta) AS asedio_id
    FROM marcas_nuevo_asedio
)

SELECT 
    id_agresor,
    id_victima,
    id_place,
    asedio_id,
    MIN(minuto_alerta) AS hora_inicio_asedio,
    MAX(minuto_alerta) AS hora_fin_asedio,
    
    TIMESTAMP_DIFF(MAX(minuto_alerta), MIN(minuto_alerta), MINUTE) + 1 AS duracion_asedio_minutos,
    
    MIN(distancia_minima_alcanzada) AS acercamiento_maximo_durante_asedio

FROM sesiones_asedio
GROUP BY id_agresor, id_victima, id_place, asedio_id