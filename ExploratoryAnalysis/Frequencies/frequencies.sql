-- Se teve uma visagem iniciando dentro do mês será 
-- contabilizada
SELECT feed_version, feed_start_date, feed_end_date, trip_id, start_time, end_time, headway_secs, exact_times
FROM `rj-smtr.gtfs.frequencies`
WHERE
  feed_version >= '2024-01-01' -- Usado somente para diminuir a carga no banco
ORDER BY feed_start_date ASC;