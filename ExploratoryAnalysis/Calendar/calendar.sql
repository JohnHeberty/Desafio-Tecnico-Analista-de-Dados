SELECT 
  data,
  tipo_dia,
  -- subtipo_dia		              -- STRING	Subtipo de dia (ex: 'Verão').
  tipo_os,
  service_id,
  c.feed_version,
  c.feed_start_date,
  feed_end_date,
  -- versao		                    -- STRING	  Código de controle de versão do dado [SHA Github]
  -- datetime_ultima_atualizacao  -- DATETIME	Última atualização [GMT-3]
FROM `rj-smtr.planejamento.calendario` AS c, UNNEST(service_ids) AS service_id
INNER JOIN `rj-smtr.gtfs.feed_info` AS f
ON
  c.feed_version    = f.feed_version AND
  c.feed_start_date = f.feed_start_date
WHERE
  c.feed_version >= '2024-11-06' AND-- Usado somente para diminuir a carga NO banco
  data BETWEEN '2024-12-01' AND '2024-12-31'