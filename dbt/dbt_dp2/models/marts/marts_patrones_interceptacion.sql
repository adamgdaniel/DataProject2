WITH alertas AS (
    SELECT * FROM {{ ref('int_alertas_por_minuto') }}
),
relacion_lugares AS (
    SELECT DISTINCT id_place FROM {{ ref('stg_rel_places_victimas') }}
)

SELECT 
    a.id_place,
    a.id_agresor,
    EXTRACT(HOUR FROM a.minuto_alerta) AS hora_del_dia,
    
    COUNT(*) AS veces_detectado_en_esta_hora,
    MIN(a.distancia_minima_alcanzada) AS distancia_minima_historica_en_esta_hora

FROM alertas a
INNER JOIN relacion_lugares r ON a.id_place = r.id_place
GROUP BY a.id_place, a.id_agresor, hora_del_dia
ORDER BY veces_detectado_en_esta_hora DESC