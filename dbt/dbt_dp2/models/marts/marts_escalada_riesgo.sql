WITH alertas AS (
    SELECT * FROM {{ ref('int_alertas_por_minuto') }}
    WHERE DATE(minuto_alerta) >= DATE_SUB(CURRENT_DATE(), INTERVAL 37 DAY)
)

SELECT 
    id_agresor,
    id_victima,
    
    COUNTIF(DATE(minuto_alerta) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) AS alertas_ultimos_7d,
    AVG(CASE WHEN DATE(minuto_alerta) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN distancia_minima_alcanzada END) AS dist_media_ultimos_7d,
    
    COUNTIF(DATE(minuto_alerta) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 37 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY)) AS alertas_previos_30d,
    
    IF(
        COUNTIF(DATE(minuto_alerta) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) > 
        (COUNTIF(DATE(minuto_alerta) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 37 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY)) / 4), -- Dividimos entre 4 para comparar 1 semana vs media semanal del mes pasado
        TRUE, 
        FALSE
    ) AS escalada_detectada_flag

FROM alertas
GROUP BY id_agresor,id_victima