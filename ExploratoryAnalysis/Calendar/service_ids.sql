-- Será usado posteriomente para as relações
SELECT DISTINCT service_id as service_ids
FROM (
  SELECT TRIM(service_id) AS service_id
  FROM `rj-smtr.planejamento.calendario`,
  UNNEST(service_ids) AS service_id
    WHERE
      data >= '2024-12-01' AND
      data <= '2024-12-31'
) temp_service_ids
ORDER BY service_id;