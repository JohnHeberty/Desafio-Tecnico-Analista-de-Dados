/*
field name	mode	            type	    description
data		                    DATE	    Data
tipo_dia		                STRING	    Dia da semana considerado para o cálculo da distância planejada - categorias: Dia Útil, Sábado, Domingo
servico		                    STRING	    Serviço planejado
consorcio		                STRING	    Consórcio ao qual o serviço pertence.
vista		                    STRING	    Itinerário do serviço (ex: Bananal ↔ Saens Peña)
sentido		                    STRING	    Sentido planejado - categorias: I (ida), V (volta), C (circular)
partidas_total_planejada		INTEGER	    Quantidade de partidas planejadas
distancia_planejada		        FLOAT	    Distância do shape (trajeto) planejado em KM
distancia_total_planejada		FLOAT	    Distância total planejada do serviço em KM (junta ida+volta).
inicio_periodo		            DATETIME	Início do período de operação planejado
fim_periodo		                DATETIME	Fim do período de operação planejado
faixa_horaria_inicio		    DATETIME	Horário inicial da faixa horária
faixa_horaria_fim		        DATETIME	Horário final da faixa horária
trip_id_planejado		        STRING	    Código identificador de trip de referência no GTFS
trip_id		                    STRING	    Código identificador de trip de referência no GTFS com ajustes
shape_id		                STRING	    Código identificador de shape no GTFS
shape_id_planejado		        STRING	    Código identificador de shape no GTFS com ajustes
data_shape		                DATE	    Data do shape capturado no SIGMOB (00h) (Válida até 2024-03-30).
shape		                    GEOGRAPHY	Linestring dos pontos gelocalizados do trajeto
sentido_shape		            STRING	    Sentido do shape (codificado no shape_id - categorias: I (ida), V (volta), C (circular)
start_pt		                GEOGRAPHY	Ponto inicial do shape em formato geográfico (Point).
end_pt		                    GEOGRAPHY	Ponto final do shape em formato geográfico (Point).
id_tipo_trajeto		            INTEGER	    Tipo de trajeto (0 - Regular, 1 - Alternativo, válida a partir de 2024-04-01).
feed_version		            STRING	    String que indica a versão atual do conjunto de dados GTFS (Válida a partir de 2024-04-01).
feed_start_date		            DATE	    Data inicial do feed do GTFS [Válida a partir de 2024-04-01]
datetime_ultima_atualizacao		DATETIME    Última atualização [GMT-3]
*/