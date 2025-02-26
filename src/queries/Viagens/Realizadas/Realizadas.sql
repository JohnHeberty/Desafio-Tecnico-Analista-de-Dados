BEGIN
    DECLARE feed_version_filter DEFAULT '2024-11-06';   -- indica a versão atual do conjunto de dados, 
                                                        -- usado para acelerar a consulta, o valor encontrato
                                                        -- e com base na Viagens_Planejadas. Não precisamos 
                                                        -- vasculhar todos os conjuntos de dados, somente apartir
                                                        -- desse informado.

    with Viagens_Realizadas AS (
        SELECT 
            ROW_NUMBER() OVER (PARTITION BY consorcio, servico_realizado, trip_id, shape_id, sentido  ORDER BY iteration ASC) AS viagens_consecutivas,
            *
        FROM (
            SELECT 
                ROW_NUMBER() OVER () AS iteration,
                *
            FROM (
                SELECT
                    DISTINCT
                    -- viagens_consecutivas,               -- Parametro criado para atualizar os datetime partida e chegada corretamente
                    consorcio,		                    -- STRING   Consórcio ao qual o serviço pertence
                    data,		                        -- DATE     Data da viagem
                    tipo_dia,		                    -- STRING   Dia da semana considerado para o cálculo da distância planejada - categorias: Dia Útil, Sábado, Domingo
                    -- id_empresa,		                -- STRING   Código identificador da empresa que opera o veículo
                    -- id_veiculo,		                -- STRING   Código identificador do veículo (número de ordem)
                    SUBSTRING(
                    id_viagem, 
                    STRPOS(id_viagem, '-') + 1
                    ) AS 
                    id_viagem,		                    -- STRING   Código identificador da viagem (ex: id_veiculo + servico + sentido + shape_id + datetime_partida)
                    -- servico_informado,		        -- STRING   Serviço informado pelo GPS do veículo
                    servico_realizado,		            -- STRING   Serviço realizado pelo veículo (com base na identificação do trajeto)
                    vista,		                        -- STRING   Texto que aparece na sinalização identificando o destino da viagem aos passageiros
                    trip_id,		                    -- STRING   Código identificador do itinerário operado
                    shape_id,		                    -- STRING   Código identificador do trajeto (shape) operado
                    sentido,		                    -- STRING   Sentido do trajeto identificado - categorias: I (ida), V (volta), C (circular)
                    datetime_partida,		            -- DATETIME Horário de início da viagem
                    datetime_chegada,		            -- DATETIME Horário de fim da viagem
                    CAST(
                        inicio_periodo AS DATE
                    ) AS inicio_periodo,		        -- DATETIME Início do período de operação planejado
                    CAST(
                        fim_periodo AS DATE
                    ) AS fim_periodo,		            -- DATETIME Fim do período de operação planejado
                    -- tipo_viagem,		                -- STRING   Tipo de viagem - categorias: Completa linha correta, Completa linha incorreta
                    tempo_viagem,		                -- INTEGER  Tempo aferido da viagem (em minutos)
                    -- tempo_planejado,		            -- INTEGER  Tempo planejado da viagem (em minutos)
                    -- distancia_planejada,		        -- FLOAT    Distância do shape (trajeto) planejado
                    -- distancia_aferida,		        -- FLOAT    Distância aferida da viagem (geodésia entre posições consecutivas do sinal de GPS)
                    -- n_registros_shape,		        -- INTEGER  Contagem de sinais de GPS emitidos dentro do trajeto.
                    -- n_registros_total,		        -- INTEGER  Contagem de sinais de GPS emitidos no tempo da viagem.
                    -- n_registros_minuto,		        -- INTEGER  Contagem de minutos do trajeto com pelo menos 1 sinal de GPS emitido.
                    -- velocidade_media,		        -- FLOAT	Velocidade média da viagem [km/h]
                    -- perc_conformidade_shape,		    -- FLOAT	Percentual de sinais emitidos dentro do shape (trajeto) ao longo da viagem
                    -- perc_conformidade_distancia,	    -- LOAT	    Razão da distância aferida pela distância teórica x 100
                    -- perc_conformidade_registros,     -- FLOAT    Percentual de minutos da viagem com registro de sinal de GPS
                    -- perc_conformidade_tempo,         -- INTEGER  Razão do tempo aferido da viagem pelo planejado x 100
                    feed_version,		                -- STRING   String que indica a versão atual do conjunto de dados gtfs.
                    feed_start_date,		            -- DATE     Data de referência do feed (versão).
                    feed_end_date,		                -- DATE     O conjunto de dados fornece informações de programação completas e confiáveis ​​para serviço no período que vai do início do dia feed_start_date até o final do dia feed_end_date.
                    v.versao_modelo,		            -- STRING   Código de controle de versão (SHA do GitHub).
                    -- ########################################################################################################################
                    -- v.versao_modelo AS versao_modelo_1, # Esses 2 Casos foram usados para Debugar, pois qualquer JOIN resultava em tudo NULL
                    -- f.versao_modelo AS versao_modelo_2, # Assim, foi identificado que existe espaços em branco no final dessas colunas.
                    -- ########################################################################################################################
                    -- CAST(
                    --   v.datetime_ultima_atualizacao AS DATE
                    --   ) as 
                    --   datetime_ultima_atualizacao,    -- DATETIME	  Última atualização [GMT-3]  
                    -- CAST(feed_update_datetime AS DATE) as feed_update_datetime 
                FROM `rj-smtr.projeto_subsidio_sppo.viagem_completa` v
                INNER JOIN `rj-smtr.gtfs.feed_info` f
                ON
                    -- TRIM(v.versao_modelo)    =   f.versao_modelo     AND     -- NÃO USAR, SE USAR SO RETORNA SABADO E DOMINGO!
                    f.feed_version              >= feed_version_filter          -- Usado para diminuir a carga NO banco
                    AND inicio_periodo          >=  feed_start_date         
                    AND fim_periodo             <=  feed_end_date
                WHERE
                    feed_version                >= feed_version_filter          -- Usado para diminuir a carga NO banco
                    AND data                    BETWEEN '2024-12-01'    AND '2024-12-31'  
                    AND tempo_viagem            > 0
                ORDER BY
                    data ASC, 
                    trip_id ASC, 
                    datetime_partida ASC
            )
        )
        ORDER BY
            iteration ASC
    )
    
    SELECT *
    FROM Viagens_Realizadas;

END;