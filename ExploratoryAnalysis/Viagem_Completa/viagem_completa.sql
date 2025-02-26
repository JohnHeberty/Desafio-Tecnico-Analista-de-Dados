-- direction_id, 0 ida, 1 volta
-- trip_headsign nome da linha
-- trip_id identificador de uma viagem
-- service_id serviços prestado 
-- route_id, id da rota
SELECT
  *
FROM
  rj-smtr.projeto_subsidio_sppo.viagem_completa AS viagem_completa
WHERE
  viagem_completa.data >= '2024-12-01'
  AND viagem_completa.data <= '2024-12-31';

ExploratoryAnalysis