-- Visualização dos Tipos de Ocorrências possiveis
-- dentro do periodo analisado
SELECT DISTINCT subtipo_dia
FROM `rj-smtr.planejamento.calendario`
WHERE 
  data >= '2024-12-01' AND
  data <= '2024-12-31' AND
  subtipo_dia IS NOT NULL
ORDER BY subtipo_dia;