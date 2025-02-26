-- Agencias Solicitadas para a analise
-- Não foi usado o feed_end_date pois ficaria
-- um buraco nos dados capturados, assim fixei
-- somente se a viagem iniciou no mês faz parte 
-- dele
SELECT DISTINCT agency_id
FROM (
  SELECT 
    * 
  FROM 
    `rj-smtr.gtfs.agency` 
  WHERE
    agency_name in ('Internorte','Intersul','Santa Cruz','Transcarioca') AND
    feed_start_date >= '2024-12-01' AND
    feed_start_date <= '2024-12-31'
  ORDER BY agency_id
) as agency;