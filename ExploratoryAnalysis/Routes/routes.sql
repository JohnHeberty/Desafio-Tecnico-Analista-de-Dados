-- Não foi usado feed_start_date e nem feed_end_date pois e onde
-- começa a viagem e finaliza a viagem. Há casos de iniciar em um 
-- dia e finalizar em outro, mas o feed_version e a data de relato
-- da informação, também pode ser usado feed_start_date para o 
-- intervalo do mês, importante e a viagem ter sido inicada na 
-- quele dia
SELECT *
FROM `rj-smtr.gtfs.routes`
WHERE
  feed_start_date >= '2024-12-01' AND
  feed_start_date <= '2024-12-31'
ORDER BY feed_start_date