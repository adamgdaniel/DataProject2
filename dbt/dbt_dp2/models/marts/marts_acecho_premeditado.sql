WITH alertas AS (
    SELECT * FROM {{ ref('int_alertas_por_minuto') }}
),
relacion_lugares AS (
    SELECT * FROM {{ ref('stg_rel_places_victimas') }}
)

SELECT 
    a.id_agresor,
    a.id_victima,
    COUNT(a.minuto_alerta) AS total_alertas_historicas,
    
    COUNT(r.id_place) AS alertas_en_safe_place_victima,
    
    ROUND(SAFE_DIVIDE(COUNT(r.id_place), COUNT(a.minuto_alerta)) * 100, 2) AS porcentaje_targeting,
    
    IF(SAFE_DIVIDE(COUNT(r.id_place), COUNT(a.minuto_alerta)) >= 0.5, TRUE, FALSE) AS acecho_premeditado_flag

FROM alertas a
LEFT JOIN relacion_lugares r 
    ON a.id_place = r.id_place 
    AND a.id_victima = r.id_victima
GROUP BY a.id_agresor, a.id_victima