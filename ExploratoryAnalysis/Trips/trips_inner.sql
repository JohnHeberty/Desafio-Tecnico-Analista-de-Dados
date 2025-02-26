-- direction_id, 0 Ida e 1 e Volta
-- service_id, serviço que a rota presta
-- trip_id id unico da viagem
-- filtrando somente pelo service id solicitados 
SELECT
  trips.trip_id,
  trips.route_id,
  trips.service_id,
  trips.feed_version,
  trips.feed_start_date,
  trips.feed_end_date
FROM
  rj-smtr.gtfs.trips AS trips
INNER JOIN
  rj-smtr.projeto_subsidio_sppo.viagem_completa AS viagem_completa
ON
  viagem_completa.trip_id = trips.trip_id
WHERE
  viagem_completa.data >= '2024-12-01'
  AND viagem_completa.data <= '2024-12-31'
  AND trips.service_id IN ('D',
    'D_REG',
    'EXCEP',
    'S_REG',
    'U_REG');
