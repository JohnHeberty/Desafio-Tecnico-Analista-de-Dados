DECLARE feed_version_filter         DEFAULT         '2024-11-06';   -- indica a versão atual do conjunto de dados, usado
                                                                    -- para  acelerar  a  consulta,  o  valor encontrato
                                                                    -- e  com  base  na  na tabela acima. Não precisamos 
                                                                    -- vasculhar  todos  os  conjuntos de dados, somente 
                                                                    -- apartir desse informado.

DECLARE start_date_view             DEFAULT DATE    '2024-12-01';  -- Data de inicio da Analise
DECLARE end_date_view               DEFAULT DATE    '2024-12-31';  -- Data de final  da Analise

DECLARE gtfs_feeds                  ARRAY<DATE>;                    -- Datas dos feeds corretos a serem usados
DECLARE gtfs_service_ids            ARRAY<STRING>;                  -- Services Ids corretos a serem usados

DECLARE association_factor FLOAT64  DEFAULT 0.25;                   -- Fator de Assosiação de 25%, Pois Será usado 25% para cima e 25% para baixo resultando em 50% de desvio!
DECLARE consecutive_factor INT64    DEFAULT 5;                      -- Fator de Consecutividade, aceita assossiar uma viagem com outra consecutiva caso ocorra atrasos! Para cima e para Baixo!

-- Criando as Datas dos Feeds correstos para usar!
SET gtfs_feeds = (
SELECT ARRAY_AGG(DISTINCT feed_start_date ORDER BY feed_start_date)
FROM `rj-smtr.planejamento.calendario`
WHERE 
    feed_version >= feed_version_filter
    AND data BETWEEN start_date_view AND end_date_view
);

-- Criando as Service_Ids correstos para usar!
SET gtfs_service_ids = (
    SELECT 
    ARRAY_AGG(
        DISTINCT service_id 
        ORDER BY service_id
    )                               -- Usando ARRAY_AGG para criar um array de service_ids
    -- data,                        --  DATE	    Data
    -- tipo_dia,                    --  STRING    Dia da semana - categorias: Dia Útil, Sábado, Domingo
    -- tipo_os,                     --  STRING	  Subtipo de dia (ex: 'Verão')
    -- services_id,                 --  STRING	  Lista de service_ids válidos para o dia
    -- feed_version,                --  STRING	  String que indica a versão atual do conjunto de dados GTFS.
    -- feed_start_date,             --  DATE	    (Partição) Data inicial do feed (versão).
    -- versao as versao_modelo,		  --  STRING    Código de controle de versão do dado [SHA Github]
    -- datetime_ultima_atualizacao, --  DATETIME  Última atualização [GMT-3]
    -- feed_publisher_name,		      --  STRING    Nome completo da organização que publica o conjunto de dados.
    -- feed_publisher_url,		      --  STRING    URL do site da organização que publica o conjunto de dados.
    -- feed_lang,		                --  STRING    Idioma padrão usado para o texto neste conjunto de dados.
    -- default_lang,		            --  STRING    Define o idioma que deve ser usado quando o consumidor de dados não conhece o idioma do passageiro.
    -- feed_contact_email,		      --  STRING    Endereço de e-mail para comunicação sobre o conjunto de dados gtfs e práticas de publicação de dados.
    -- feed_contact_url,		        --  STRING    URL para informações de contato, um formulário web, uma mesa de suporte ou outras ferramentas de comunicação relativas ao conjunto de dados GTFS e práticas de publicação de dados.
    FROM `rj-smtr.planejamento.calendario` AS c, UNNEST(c.service_ids) AS service_id 
    WHERE 
        feed_version >= feed_version_filter
        AND data BETWEEN start_date_view AND end_date_view
);

-- viagens planejadas
WITH temp_trips AS (
    SELECT
        DISTINCT                    --  Não fez efeito, rever novamente para retirar, sem necessidade!
        feed_version,		            --  STRING	  String que indica a versão atual do conjunto de dados gtfs.
        feed_start_date,		        --  DATE	    (Partição) Data inicial do feed (versão).
        feed_end_date,		          --  DATE	    Data final do feed (versão).
        route_id,		                --  STRING    Identifica uma rota.
        service_id,		              --  STRING	  Identifica um conjunto de datas em que o serviço está disponível para uma ou mais rotas.
        trip_id,		                --  STRING    Identifica uma viagem à qual se aplica o intervalo de serviço especificado.
        trip_headsign,              --  STRING    Texto que aparece na sinalização identificando o destino da viagem aos passageiros.
        trip_short_name,            --  STRING    Texto voltado ao público usado para identificar a viagem aos passageiros, por exemplo, para identificar os números dos trens para viagens de trens suburbanos.
        direction_id,               --  STRING    Indica a direção de deslocamento de uma viagem.
        -- block_id,		            --  STRING    Identifica o bloco ao qual pertence a viagem.
        shape_id,		                --  STRING    Identifica uma forma geoespacial que descreve o caminho de deslocamento do veículo para uma viagem.
        -- wheelchair_accessible,   --  STRING    Indica acessibilidade para cadeiras de rodas.
        -- bikes_allowed,		        --  STRING    Indica se bicicletas são permitidas.
        versao_modelo		            --  STRING    Código de controle de versão (SHA do GitHub).
    FROM
        `rj-smtr.gtfs.trips`
    WHERE
        feed_version          >= feed_version_filter -- Usado para diminuir a carga NO banco
        AND feed_start_date   IN UNNEST(gtfs_feeds)
),
-- frequencias das viagens planejadas
temp_frequencies AS (
    SELECT
        DISTINCT                    --  Não fez efeito, rever novamente para retirar, sem necessidade!
        feed_version,               --  STRING    String que indica a versão atual do conjunto de dados gtfs.
        feed_start_date,            --  DATE	    (Partição) Data inicial do feed (versão). 
        feed_end_date,              --  DATE	    Data final do feed (versão).
        trip_id,                    --  STRING    Identifica uma viagem à qual se aplica o intervalo de serviço especificado.
        start_time,                 --  STRING    Hora em que o primeiro veículo sai da primeira parada da viagem com o intervalo especificado.
        end_time,                   --  STRING    Hora em que o serviço muda para um intervalo diferente (ou cessa) na primeira parada da viagem.
        headway_secs,               --  INTEGER   Tempo, em segundos, entre partidas da mesma parada (intervalo) da viagem, durante o intervalo de tempo especificado por start_time e end_time.
        -- exact_times,             --  STRING    Indica o tipo de serviço para uma viagem.
        versao_modelo		            --  STRING	  Código de controle de versão (SHA do GitHub).
    FROM
        `rj-smtr.gtfs.frequencies`
    WHERE
        feed_version >= feed_version_filter -- Usado somente para diminuir a carga NO banco
        AND feed_start_date IN UNNEST(gtfs_feeds)
),
-- rotas das viagens planejadas
temp_router AS (
    SELECT
        DISTINCT                      -- DIMINUIU PELA METADE AS LINHAS!
        r.feed_version,               -- STRING	  String que indica a versão atual do conjunto de dados gtfs.
        r.feed_start_date,            -- DATE	    (Partição) Data inicial do feed (versão).
        r.feed_end_date,              -- DATE	    Data final do feed (versão).
        r.route_id,                   -- STRING	  Identifica uma rota.
        r.agency_id,                  -- STRING	  Agência para a rota especificada.
        a.agency_name,                -- STRING	  Nome completo da agência de transporte público.
        r.route_short_name,          -- STRING   Nome abreviado de uma rota.
        -- route_long_name,           -- STRING   Nome completo de uma rota.
        -- route_desc,                -- STRING   Descrição de uma rota que fornece informações úteis e de qualidade.
        -- route_type,                -- STRING   Indica o tipo de transporte utilizado em uma rota.
        -- route_url,		              -- STRING	  URL de uma página da web sobre uma rota específica.
        -- route_color,		            -- STRING	  Designação de cores da rota que corresponda ao material voltado para o público.
        -- route_text_color,	        -- STRING	  Cor legível a ser usada para texto desenhado contra um fundo de route_color.
        -- route_sort_order,	        -- INTEGER  Ordena as rotas de forma ideal para apresentação aos clientes.
        -- continuous_pickup,		      -- STRING	  Indica que o passageiro pode embarcar no veículo de transporte público em qualquer ponto ao longo do trajeto de viagem do veículo, conforme descrito em shape.txt, em cada viagem do trajeto.
        -- continuous_drop_off,       -- STRING	  Indica que o passageiro pode descer do veículo de transporte público em qualquer ponto ao longo do trajeto de viagem do veículo, conforme descrito em shape.txt, em cada viagem da rota.
        -- network_id,		            -- STRING	  Identifica um grupo de rotas. Várias linhas em rotas.txt podem ter o mesmo network_id.
        r.versao_modelo,		          -- STRING   Código de controle de versão (SHA do GitHub).
    FROM
        `rj-smtr.gtfs.routes` as r
    LEFT JOIN `rj-smtr.gtfs.agency` as a ON
        r.agency_id           = a.agency_id 
        AND a.feed_version    >= feed_version_filter -- Usado somente para diminuir a carga NO banco
        AND a.agency_name     IN ('Internorte', 'Intersul', 'Santa Cruz', 'Transcarioca')
        AND r.versao_modelo   = a.versao_modelo
        AND r.feed_start_date IN UNNEST(gtfs_feeds)
    WHERE
        r.feed_version >= feed_version_filter  -- Usado somente para diminuir a carga NO banco
        AND r.feed_start_date IN UNNEST(gtfs_feeds)
),
-- calendario das viagens planejadas com feed_info p/ pegar a coluna feed_end_date
temp_calendar AS (
    SELECT
    DISTINCT                      --  Não fez efeito, rever novamente para retirar, sem necessidade!
    c.data,                       --  DATE	    Data
    -- tipo_dia,                  --  STRING    Dia da semana - categorias: Dia Útil, Sábado, Domingo
    -- subtipo_dia		          --  STRING	Subtipo de dia (ex: 'Verão').
    tipo_os,                      --  STRING	  Subtipo de dia (ex: 'Verão')
    service_id,                   --  STRING	  Lista de service_ids válidos para o dia
    c.feed_version,               --  STRING	  String que indica a versão atual do conjunto de dados GTFS.
    c.feed_start_date,            --  DATE	    (Partição) Data inicial do feed (versão).
    -- f.feed_end_date,           --  DATE	    O conjunto de dados fornece informações de programação completas e confiáveis ​​para serviço no período que vai do início do dia feed_start_date até o final do dia feed_end_date.
    versao as versao_modelo,	  --  STRING    Código de controle de versão do dado [SHA Github]
    -- f.versao_modelo as VM2,    --  STRING	  Código de controle de versão (SHA do GitHub).
    datetime_ultima_atualizacao,  --  DATETIME  Última atualização [GMT-3]
    -- feed_publisher_name,		  --  STRING    Nome completo da organização que publica o conjunto de dados.
    -- feed_publisher_url,		  --  STRING    URL do site da organização que publica o conjunto de dados.
    -- feed_lang,		          --  STRING    Idioma padrão usado para o texto neste conjunto de dados.
    -- default_lang,		      --  STRING    Define o idioma que deve ser usado quando o consumidor de dados não conhece o idioma do passageiro.
    -- feed_contact_email,		  --  STRING    Endereço de e-mail para comunicação sobre o conjunto de dados gtfs e práticas de publicação de dados.
    -- feed_contact_url,		  --  STRING    URL para informações de contato, um formulário web, uma mesa de suporte ou outras ferramentas de comunicação relativas ao conjunto de dados GTFS e práticas de publicação de dados.
    -- f.feed_update_datetime	  --  DATETIME  Data e hora da última atualização do feed.
    FROM `rj-smtr.planejamento.calendario` AS c, UNNEST(c.service_ids) AS service_id
    -- # NÃO PASSOU NA AUDITORIA ###############################################################################################################################
    -- INNER JOIN `rj-smtr.gtfs.feed_info` AS f
    -- ON                                         -- Tentativa de considerar dados do feed_info para trazer o feed_end_date
    --   c.feed_version    = f.feed_version AND   -- E o versao_modelo já que o versao presente nessas datas não casa com outros dados 
    --   c.feed_start_date = f.feed_start_date      
    -- #################################################################################################################################
    WHERE
    c.feed_version      >= feed_version_filter  -- Usado somente para diminuir a carga NO banco
    AND c.data          BETWEEN start_date_view AND end_date_view
    AND feed_start_date IN UNNEST(gtfs_feeds)
),
-- Pré-Processamento inicial, da atividade 1 até 3
temp_Viagens_Planejadas AS (
    SELECT *
    FROM (
        SELECT DISTINCT
            -- ROW_NUMBER() OVER (
            --   PARTITION BY 
            --     r.agency_name,
            --     t.service_id, 
            --     t.trip_id,
            --     t.shape_id, 
            --     t.direction_id, 
            --     t.feed_version, 
            --     t.feed_start_date, 
            --     t.feed_end_date, 
            --     t.versao_modelo
            --   ORDER BY data DESC
            -- ) AS rn,                                 -- ##########   Usado para criar uma chave unica da linha, assim aplica-se DISTINCT para linpar as duplicatas.
            c.data,                                     -- DATE	        Data da viagem
            r.agency_name as consorcio,                 -- STRING	    Consórcio ao qual o serviço pertence
            -- c.tipo_dia,                                 -- STRING	    Dia da semana considerado para o cálculo da distância planejada - categorias: Dia Útil, Sábado, Domingo
            -- agency_id as id_empresa,                 -- STRING	    Código identificador da empresa que opera o veículo
            -- id_veiculo                               -- STRING	    Código identificador do veículo (número de ordem)
            -- id_viagem --INCLUIDO POT .PY             -- STRING	    Código identificador da viagem (ex: id_veiculo + servico + sentido + shape_id + datetime_partida)
            -- servico_informado,		                -- STRING	    Serviço informado pelo GPS do veículo
            t.service_id as                             -- ##########   Identifica um conjunto de datas em que o serviço está disponível para uma ou mais rotas.
            servico_realizado,                          -- STRING	    Serviço planejado pelo veículo (com base na identificação do trajeto)
            t.trip_headsign AS vista,                   -- STRING	    Texto que aparece na sinalização identificando o destino da viagem aos passageiros
            t.trip_id,                                  -- STRING	    Código identificador do itinerário operado
            t.shape_id,                                 -- STRING	    Código identificador do trajeto (shape) operado
            CASE
            WHEN t.direction_id = '0' THEN 'I' 
            WHEN t.direction_id = '1' THEN 'V'
            ELSE 'C'                                    -- ##########   Indica a direção de deslocamento de uma viagem. 0 Ida e 1 e Volta.
            END AS sentido,                             -- STRING	    Sentido do trajeto identificado - categorias: I (ida), V (volta), C (circular)
            (
                c.data + 
                MAKE_INTERVAL( hour   => SAFE_CAST(SPLIT(start_time, ':')[OFFSET(0)] AS INT64)) +
                MAKE_INTERVAL( minute => SAFE_CAST(SPLIT(start_time, ':')[OFFSET(1)] AS INT64)) +
                MAKE_INTERVAL( second => SAFE_CAST(SPLIT(start_time, ':')[OFFSET(2)] AS INT64))
            ) as datetime_partida,                      -- DATETIME	    Horário de início da viagem
            (
                c.data + 
                MAKE_INTERVAL( hour   => SAFE_CAST(SPLIT(start_time, ':')[OFFSET(0)] AS INT64)) +
                MAKE_INTERVAL( minute => SAFE_CAST(SPLIT(start_time, ':')[OFFSET(1)] AS INT64)) +
                MAKE_INTERVAL( second => SAFE_CAST(SPLIT(start_time, ':')[OFFSET(2)] AS INT64)) + 
            INTERVAL TIME_DIFF(
                TIME(TIMESTAMP_SECONDS(
                    SAFE_CAST(SPLIT(end_time, ':')[OFFSET(0)] AS INT64) * 3600 +
                    SAFE_CAST(SPLIT(end_time, ':')[OFFSET(1)] AS INT64) * 60 +
                    SAFE_CAST(SPLIT(end_time, ':')[OFFSET(2)] AS INT64)
                )),
                TIME(TIMESTAMP_SECONDS(
                    SAFE_CAST(SPLIT(start_time, ':')[OFFSET(0)] AS INT64) * 3600 +
                    SAFE_CAST(SPLIT(start_time, ':')[OFFSET(1)] AS INT64) * 60 +
                    SAFE_CAST(SPLIT(start_time, ':')[OFFSET(2)] AS INT64)
                )),
                SECOND
            ) SECOND
            ) AS datetime_chegada,                      -- DATETIME     Horário de fim da viagem
            TIME_DIFF(
            TIME(
                TIMESTAMP_SECONDS(
                SAFE_CAST(SPLIT(end_time, ':')[OFFSET(0)] AS INT64) * 3600 +
                SAFE_CAST(SPLIT(end_time, ':')[OFFSET(1)] AS INT64) * 60 +
                SAFE_CAST(SPLIT(end_time, ':')[OFFSET(2)] AS INT64)
                )
            ),
            TIME(
                TIMESTAMP_SECONDS(
                SAFE_CAST(SPLIT(start_time, ':')[OFFSET(0)] AS INT64) * 3600 +
                SAFE_CAST(SPLIT(start_time, ':')[OFFSET(1)] AS INT64) * 60 +
                SAFE_CAST(SPLIT(start_time, ':')[OFFSET(2)] AS INT64)
                )
            ),
            MINUTE
            ) AS tempo_viagem,                          -- INTEGER      Tempo aferido da viagem (em minutos)
            t.route_id,                                 -- ##########   Identifica uma rota.
            -- trip_headsign,                           -- STRING	    Texto que aparece na sinalização identificando o destino da viagem aos passageiros.
            t.trip_short_name,                          -- ##########   Texto voltado ao público usado para identificar a viagem aos passageiros, por exemplo, para identificar os números dos trens para viagens de trens suburbanos.
            -- exact_times,                             -- Sessão       Não Analisada se tem alguma variavel aqui que pode virar alguma das de fora comentada
            -- route_short_name,                        -- STRING       Nome abreviado de uma rota.
            -- route_long_name,                         -- STRING       Nome completo de uma rota.
            -- route_desc,                              -- STRING       Descrição de uma rota que fornece informações úteis e de qualidade.
            -- route_type,                              -- STRING       Indica o tipo de transporte utilizado em uma rota.
            tipo_os,                                    -- STRING	    Subtipo de dia (ex: 'Verão')
            -- inicio_periodo		                    -- DATETIME	    Início do período de operação planejado
            -- fim_periodo		                        -- DATETIME	    Fim do período de operação planejado
            -- tipo_viagem		                        -- STRING	    Tipo de viagem - categorias: Completa linha correta, Completa linha incorreta
            CAST(
                CAST(
                    headway_secs AS SMALLINT            -- Menor Type Possivel, dado ta menor que 32K / +Desempenho
                ) / 60 AS SMALLINT                      -- Menor Type Possivel, dado ta menor que 32K / +Desempenho
            ) as headway,                               -- INTEGER      Tempo, em minutos, entre partidas da mesma parada (intervalo) da viagem, durante o intervalo de tempo especificado por start_time e end_time.
            -- tempo_planejado		                    -- INTEGER	    Tempo planejado da viagem (em minutos)
            -- distancia_planejada                      -- FLOAT	    Distância do shape (trajeto) planejado
            -- distancia_aferida		                -- FLOAT	    Distância aferida da viagem (geodésia entre posições consecutivas do sinal de GPS)
            -- n_registros_shape		                -- INTEGER	    Contagem de sinais de GPS emitidos dentro do trajeto.
            -- n_registros_total		                -- INTEGER	    Contagem de sinais de GPS emitidos no tempo da viagem.
            -- n_registros_minuto		                -- INTEGER	    Contagem de minutos do trajeto com pelo menos 1 sinal de GPS emitido.
            -- velocidade_media		                    -- FLOAT	    Velocidade média da viagem [km/h]
            -- perc_conformidade_shape		            -- FLOAT	    Percentual de sinais emitidos dentro do shape (trajeto) ao longo da viagem
            -- perc_conformidade_distancia              -- FLOAT	    Razão da distância aferida pela distância teórica x 100
            -- perc_conformidade_registros              -- FLOAT	    Percentual de minutos da viagem com registro de sinal de GPS
            -- perc_conformidade_tempo		            -- INTEGER	    Razão do tempo aferido da viagem pelo planejado x 100
            t.feed_version,                             -- ##########   Usado somente para unir as versões dos dados corretamente / indica a versão atual do conjunto de dados
            t.feed_start_date,                          -- ##########   Usado somente para unir as versões dos dados corretamente / (Partição) Data inicial do feed (versão).
            t.feed_end_date,                            -- ##########   Usado somente para unir as versões dos dados corretamente / (Partição) Data final do feed (versão).
            t.versao_modelo                             -- STRING	    Versão da metodologia de cálculo da respectiva linha na tabela
            -- CAST(
            --  c.datetime_ultima_atualizacao AS DATE
            -- ) as 
            -- datetime_ultima_atualizacao,             -- DATETIME	    Última atualização [GMT-3]
            -- CAST(feed_update_datetime AS DATE) as feed_update_datetime
        FROM
            temp_trips AS t
        -- # # # EXPLICAÇÃO NO TOPICO - ANALISE - 1 # # #
        INNER JOIN
            temp_frequencies AS f
        ON
            t.feed_version          = f.feed_version    
            AND t.feed_start_date   = f.feed_start_date 
            AND t.feed_end_date     = f.feed_end_date   
            AND t.trip_id           = f.trip_id         
            AND t.versao_modelo     = f.versao_modelo         
        -- # # # EXPLICAÇÃO NO TOPICO - ANALISE - 2 # # #
        RIGHT JOIN
            temp_router AS r
        ON
            t.feed_version          = r.feed_version    
            AND t.feed_start_date   = r.feed_start_date 
            AND t.feed_end_date     = f.feed_end_date   
            AND t. route_id         = r.route_id        
            AND t. versao_modelo    = r.versao_modelo
        -- # # # EXPLICAÇÃO NO TOPICO - ANALISE - 3 # # # ###################################################################
        INNER JOIN temp_calendar AS c 
        ON
            t.feed_version          =  c.feed_version  
            AND t.feed_start_date   =  c.feed_start_date 
            -- AND t.feed_end_date   =  c.feed_end_date   
            AND t.service_id        =  c.service_id     
            -- TRIM(t.versao_modelo)   =   TRIM(c.versao_modelo)
        WHERE 
            t.feed_version                >= feed_version_filter   -- Usado somente para diminuir a carga NO banco
            AND r.feed_start_date         IN UNNEST(gtfs_feeds)
            AND TRIM(r.route_short_name)  =  TRIM(t.trip_short_name)
    )
    WHERE 
    tempo_viagem > 0    -- Foi Encontrado Viagem com data negativa, IGNORE!
    ORDER BY  -- WARNING: Usado P/ ver o resultado, se tem linhas duplicadas, após a correção e bom evitar, gera carga desnecessaria no BigQuery
    trip_id ASC,
    datetime_partida ASC,
    tempo_viagem ASC
),
-- Geração de Viagem, da atividade 4, PASSO INICIAL, EXPANSÃO DE TABELA PROPORCIONAL AO HEADWAY
temp_EXP_Viagens_Planejadas as (
    SELECT 
        DISTINCT
        *
    FROM temp_Viagens_Planejadas v
    JOIN UNNEST(
    GENERATE_ARRAY(
        1, 
        CAST(
        SAFE_DIVIDE(
            v.tempo_viagem, 
            v.headway
        ) 
        AS INT64)
    )
    ) AS i
),
-- Criando agrupamento por trip_id para usar como base no multiplicador de horario de data inicio e fim de viagem
-- Tablea Escolhida para Ser Armazenada na tabela temporaria
--
-- Pré-Processamento realizou Expansão da tabela duplicando linhas para gerar viagens
-- Neste item, realizamos atualização dos dados de partida e chegada.
GenerateTrips AS (
    SELECT
        row_id,
        consorcio,                -- STRING	    Consórcio ao qual o serviço pertence
        data,                     -- DATE	      Data da viagem
        -- tipo_dia,                 -- STRING	    Dia da semana considerado para o cálculo da distância planejada - categorias: Dia Útil, Sábado, Domingo
        trip_short_name as 
        servico_realizado,        -- ########## Texto voltado ao público usado para identificar a viagem aos passageiros, por exemplo, para identificar os números dos trens para viagens de trens suburbanos.
        vista,                    -- STRING	    Texto que aparece na sinalização identificando o destino da viagem aos passageiros
        trip_id,                  -- STRING	    Código identificador do itinerário operado
        shape_id,                 -- STRING	    Código identificador do trajeto (shape) operado
        sentido,                  -- STRING	    Sentido do trajeto identificado - categorias: I (ida), V (volta), C (circular)
        CAST(datetime_partida AS DATETIME) + INTERVAL (headway * (row_id    ) ) MINUTE AS datetime_partida,         -- DATETIME	  Horário de início da viagem
        CAST(datetime_partida AS DATETIME) + INTERVAL (headway * (row_id + 1) ) MINUTE AS datetime_chegada,         -- DATETIME	  Horário de fim da viagem
        tempo_viagem,             -- INTEGER	  Tempo aferido da viagem (em minutos)
        -- headway,                  -- INTEGER    Tempo, em minutos, entre partidas da mesma parada (intervalo) da viagem, durante o intervalo de tempo especificado por start_time e end_time.
        feed_version,             -- ########## Usado somente para unir as versões dos dados corretamente / indica a versão atual do conjunto de dados
        feed_start_date,          -- ########## Usado somente para unir as versões dos dados corretamente / (Partição) Data inicial do feed (versão).
        feed_end_date,            -- ########## Usado somente para unir as versões dos dados corretamente / (Partição) Data final do feed (versão).
        -- versao_modelo             -- STRING	    Versão da metodologia de cálculo da respectiva linha na tabela 
    FROM (
        SELECT 
            ROW_NUMBER() OVER (PARTITION BY trip_id, datetime_partida, servico_realizado  ORDER BY iteration ASC) AS row_id,
            *
        FROM (
            SELECT 
                ROW_NUMBER() OVER () AS iteration,
                *
            FROM (
                SELECT
                    DISTINCT
                    *
                FROM 
                    temp_EXP_Viagens_Planejadas
                ORDER BY                            -- SE ALTERAR A ORDER AQUI TEM QUE ALTERAR EM VIAGEM REALIZADA TAMBÉM
                    trip_id ASC, 
                    datetime_partida ASC,
                    servico_realizado ASC
            )
        )
    )

),
-- Criando o ID_Viagem!
EndGenerateTrips AS (
    SELECT 
        DISTINCT
        row_id as viagens_consecutivas,
        consorcio,
        data,
        -- tipo_dia,
        CONCAT(
            servico_realizado, '-', 
            sentido, '-', 
            shape_id, '-', 
            CAST(
                FORMAT_TIMESTAMP('%Y%m%d%H%M%S', datetime_partida)
            AS STRING)
        ) AS id_viagem,                       -- STRING	    Código identificador da viagem (ex: id_veiculo + servico + sentido + shape_id + datetime_partida)
        servico_realizado,
        vista,
        trip_id,
        shape_id,
        sentido,
        datetime_partida,
        datetime_chegada,
        tempo_viagem,
        feed_version,             -- ########## Usado somente para unir as versões dos dados corretamente / indica a versão atual do conjunto de dados
        feed_start_date,          -- ########## Usado somente para unir as versões dos dados corretamente / (Partição) Data inicial do feed (versão).
        -- versao_modelo,
        -- *
    FROM GenerateTrips
    ORDER BY
        data ASC, 
        trip_id DESC, 
        datetime_partida DESC
),
-- VIAGENS REALIZADAS!
Viagens_Realizadas AS (
    SELECT 
        ROW_NUMBER() OVER (PARTITION BY data, trip_id, data, servico_realizado  ORDER BY iteration ASC) AS viagens_consecutivas,
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
                -- tipo_dia,		                    -- STRING   Dia da semana considerado para o cálculo da distância planejada - categorias: Dia Útil, Sábado, Domingo
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
                -- v.versao_modelo,		            -- STRING   Código de controle de versão (SHA do GitHub).
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
            ORDER BY                            -- SE ALTERAR A ORDER AQUI TEM QUE ALTERAR EM GenerateTrips TAMBÉM
                trip_id ASC, 
                datetime_partida ASC,
                servico_realizado ASC
        )
    )
    ORDER BY
        iteration ASC
),

-- MERGE 
Viagens_MERGE AS (
    SELECT DISTINCT
        -- VP.feed_version,                                    -- ########## Usado somente para unir as versões dos dados corretamente / indica a versão atual do conjunto de dados
        -- VP.feed_start_date,                                 -- ########## Usado somente para unir as versões dos dados corretamente / (Partição) Data inicial do feed (versão).
        -- VP.consorcio,
        -- VP.data as data_VP,
        -- VR.data as data_VR,
        VP.servico_realizado as servico_VP,
        VR.servico_realizado as service_VR,
        -- VP.viagens_consecutivas as viagens_consecutivas_VP,
        -- VR.viagens_consecutivas as viagens_consecutivas_VR,
        -- VP.data as DT_PL,
        -- VR.data as DT_RL,
        -- VP.vista as vista_VP,
        -- VR.vista as vista_VR,
        -- VP.trip_id as trip_id_VP,
        -- VR.trip_id as trip_id_VR,
        -- VP.shape_id as shape_id_VP,
        -- VR.shape_id as shape_id_VR,
        -- VP.sentido as sentido_VP,
        -- VR.sentido as sentido_VR,
        VP.datetime_partida as datetime_partida_VP,
        VR.datetime_partida as datetime_partida_VR,
        VP.datetime_chegada as datetime_chegada_VP,
        VR.datetime_chegada as datetime_chegada_VR,
        VP.tempo_viagem as tempo_viagem_VP,
        VR.tempo_viagem as tempo_viagem_VR,
        -- VP.versao_modelo as versao_modelo_VP,
        -- VR.versao_modelo as versao_modelo_VR,
        -- *
    FROM Viagens_Realizadas         AS VR
    LEFT JOIN EndGenerateTrips      AS VP           -- PQ LEFT ? SIMPLES QUEREMOS AS CORRESPONDENCIAS DAS PLANEJADAS APARTIR DAS REALIZDAS!
    ON
        -- VP.feed_version             =       VR.feed_version
        -- AND VP.feed_start_date      =       VR.feed_start_date
        -- AND 
        VP.consorcio                =       VR.consorcio
        AND VP.trip_id              =       VR.trip_id
        AND VP.sentido              =       VR.sentido
        -- AND VP.shape_id             =       VR.shape_id
        AND VP.servico_realizado    =       VR.servico_realizado
        AND VP.tempo_viagem         > 0
        AND VR.tempo_viagem         > 0
        
        -- ############## ASSOSIANDO PELO FATOR ## COM DATA + INTERVALO ## ALÉM DE AVALIAR O INTERVALOR SOZINHO ###################################################
        --  INTERVALO DE CONFIANÇA DE 50%, DISPERSANDO 25% PARA CIMA E 25% PARA BAIXO
        --  
        --  VP.datetime_partida - VP.tempo_viagem * (1-association_factor) <= VR.datetime_partida <= VP.datetime_partida + VP.tempo_viagem * (1+association_factor) 
        AND VP.datetime_partida BETWEEN 
            (VR.datetime_partida - MAKE_INTERVAL( minute => SAFE_CAST(VR.tempo_viagem * association_factor AS INT64) ) ) 
            AND 
            (VR.datetime_partida + MAKE_INTERVAL( minute => SAFE_CAST(VR.tempo_viagem * association_factor AS INT64) ) )
        --  INTERVALO DE CONFIANÇA DE 50%, DISPERSANDO 25% PARA CIMA E 25% PARA BAIXO
        --  
        --  VP.tempo_viagem * (1-association_factor) <= VR.tempo_viagem <= VP.tempo_viagem * (1+association_factor)
        -- AND (ABS(VR.tempo_viagem - VP.tempo_viagem)/VR.tempo_viagem) <= (association_factor * 2) -- INTERVALO DE CONFIANÇA PARA EVITAR FALHAS
        AND ABS(VR.viagens_consecutivas - VP.viagens_consecutivas) <= consecutive_factor -- INTERVALO DE CONFIANÇA PARA EVITAR FALHAS
        -- ########################################################################################################################################################
    -- ORDER BY
    --     VP.viagens_consecutivas ASC, 
    --     VR.trip_id DESC, 
    --     VR.datetime_partida DESC
)

-- IGNORANDO FEED_VERSION NULO, Se a versão do dado for NULL ignore pois provavelmente está errado. 
SELECT *
FROM Viagens_MERGE
WHERE
        tempo_viagem_VP             IS NOT NULL
    AND tempo_viagem_VR             IS NOT NULL

---------------------------------------------------------------------------------------
-- # # # ANALISE - 1 # # #
-- Aferindo as linhas das uniões dos dados
-- STATUS USO   LINHAS
-- RIGHT JOIN   2677525
-- LEFT JOIN    2686640
-- INNER JOIN   2677525
--
-- OBS:
-- Resultado dessa analise e que temos dados nulos
-- ou valores diferentes na tabela trips comprado 
-- com a tabela frequencies, significa que a tabela
-- frequencies esta contida em trips mas trips não
-- esta contido em frequencies. A perca de dados
-- estimada e aproximadamente 0.34%, não e muito
-- significativo, mas e dado perdido. 
---------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------
-- # # # ANALISE - 2 # # #
-- Aferindo as linhas das uniões dos dados
-- STATUS USO   LINHAS
-- RIGHT JOIN   2677525
-- LEFT JOIN    2677525
-- INNER JOIN   2677525
--
-- OBS:
-- Significa que trips está contido em routes e que
-- routes está contido em trips em 100%, nesse caso
-- será usado o RIGHT JOIN por convenção.
---------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------
-- # # # ANALISE - 3 # # #
-- Aferindo as linhas das uniões dos dados
-- STATUS USO   LINHAS
-- RIGHT JOIN   2677557
-- LEFT JOIN    3649937
-- INNER JOIN   2677525
--
-- OBS:
-- Significa que trips esta contido em calendar mas 
-- calendar não está contido em trips, apesar de que 
-- a tabela calendar foi feito uma tratativa para 
-- expandir o array que estava dentro da coluna 
-- services_id, sendo necessario, necessita-se de uma
-- maior antenção, inconclusivo.
---------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------
-- # # # ANÁLISE - 4 # # # 
-- Foi verificado que existe 9 linhas duplicadas no primeiro momento
-- e interessante que se investique o motivo, se tem alguma coisa erra
-- na query, caso não pode ser que as tabelas relacionadas podem ter 
-- linhas duplicadas, assim, fica a questão e preciso ter essas linhas
-- duplicadas ? Para resolver temporarioamente o problema foi aplicado
-- DISTINCT nas colunas.
--
-- SEM DISTINCT 2677525 LINHAS
-- COM DISTINCT 306941  LINHAS
--
-- Isso significa que tem aproximadamente 9 linhas duplicasdas, sendo
-- que há casos que pode ter menos ou mais, pois isso não foi aferido
-- nessa analise, e 2677525 / 306941 e por volta de 8.723, quase 9.
---------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------
-- # # # CONCLUSÃO # # #
-- Para esse join, será usado o INNER JOIN, pois não vamos trabalhar 
-- com dados nulos, pois, estamos buscando o feed_version start e end, 
-- no caso a versão desse dado, se não temos referencia, que dado 
-- e esse ? para evitar tal questionamento, INNER JOIN resolve e 
-- isso faz ignoramos aproximadamente 564.631 linhas, no caso
-- se aproxima de 45% dos dados.
--
-- E como foi aplicado a o distinct em 55% dos dados restantes pois
-- foi identificado por volta de 9 linhas duplicadas, isso cresce o 
-- banco de forma desnecessaria, no caso, poderiamos diminuir 55% em
-- 9 vezes ainda, restando aproximadamente 306941 linas  de 1267011
-- linhas, por volta de 24% somente.
---------------------------------------------------------------------------------------