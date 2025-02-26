-- direction_id, 0 Ida e 1 e Volta
-- service_id, serviço que a rota presta
-- trip_id id unico da viagem
-- filtrando somente pelo service id solicitados 
SELECT feed_version, feed_start_date, feed_end_date, route_id, service_id, trip_id, trip_headsign, trip_short_name, direction_id
FROM `rj-smtr.gtfs.trips`
WHERE
  feed_version >= '2024-01-01'; -- Usado para diminuir a carga no banco

