WITH alertas_limpias AS (
    SELECT * FROM {{ ref('stg_alertas') }}
)

SELECT 
    minuto_alerta,
    id_victima,
    id_agresor,
    id_place,
    
    -- Nos quedamos con el nombre del lugar (cogemos el MAX por si acaso, aunque será el mismo)
    MAX(nombre_place) AS nombre_place,
    
    -- Contamos cuántas veces saltó la pulsera/app en ese minuto exacto
    COUNT(*) AS cantidad_pings_en_minuto,
    
    -- La métrica clave: ¿Cuál fue el momento de mayor peligro en ese minuto?
    MIN(distancia_metros) AS distancia_minima_alcanzada,
    
    -- Mantenemos los límites para poder hacer cálculos después
    MAX(distancia_limite) AS distancia_limite,
    
    -- Si en algún ping de ese minuto el nivel fue "CRITICO", marcamos todo el minuto como CRITICO
    MAX(nivel) AS nivel_maximo_minuto,
    
    -- Nos guardamos la última dirección de escape registrada en ese minuto
    MAX(direccion_escape) AS direccion_escape_minuto

FROM alertas_limpias
GROUP BY 
    minuto_alerta,
    id_victima,
    id_agresor,
    id_place