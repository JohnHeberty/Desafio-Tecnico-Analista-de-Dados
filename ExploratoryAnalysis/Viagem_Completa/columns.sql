/*
field name	mode	            type	    description
consorcio		                STRING	    Consórcio ao qual o serviço pertence
data		                    DATE	    Data da viagem
tipo_dia		                STRING      Dia da semana considerado para o cálculo da distância planejada - categorias: Dia Útil, Sábado, Domingo
id_empresa		                STRING	    Código identificador da empresa que opera o veículo
id_veiculo		                STRING	    Código identificador do veículo (número de ordem)
id_viagem		                STRING	    Código identificador da viagem (ex: id_veiculo + servico + sentido + shape_id + datetime_partida)
servico_informado		        STRING	    Serviço informado pelo GPS do veículo
servico_realizado		        STRING	    Serviço realizado pelo veículo (com base na identificação do trajeto)
vista		                    STRING	    Texto que aparece na sinalização identificando o destino da viagem aos passageiros
trip_id		                    STRING	    Código identificador do itinerário operado
shape_id		                STRING	    Código identificador do trajeto (shape) operado
sentido		                    STRING	    Sentido do trajeto identificado - categorias: I (ida), V (volta), C (circular)
datetime_partida		        DATETIME	Horário de início da viagem
datetime_chegada		        DATETIME	Horário de fim da viagem
inicio_periodo		            DATETIME	Início do período de operação planejado
fim_periodo		                DATETIME	Fim do período de operação planejado
tipo_viagem		                STRING	    Tipo de viagem - categorias: Completa linha correta, Completa linha incorreta
tempo_viagem		            INTEGER	    Tempo aferido da viagem (em minutos)
tempo_planejado		            INTEGER	    Tempo planejado da viagem (em minutos)
distancia_planejada		        FLOAT	    Distância do shape (trajeto) planejado
distancia_aferida		        FLOAT	    Distância aferida da viagem (geodésia entre posições consecutivas do sinal de GPS)
n_registros_shape		        INTEGER	    Contagem de sinais de GPS emitidos dentro do trajeto.
n_registros_total		        INTEGER	    Contagem de sinais de GPS emitidos no tempo da viagem.
n_registros_minuto		        INTEGER	    Contagem de minutos do trajeto com pelo menos 1 sinal de GPS emitido.
velocidade_media		        FLOAT	    Velocidade média da viagem [km/h]
perc_conformidade_shape		    FLOAT	    Percentual de sinais emitidos dentro do shape (trajeto) ao longo da viagem
perc_conformidade_distancia		FLOAT	    Razão da distância aferida pela distância teórica x 100
perc_conformidade_registros		FLOAT	    Percentual de minutos da viagem com registro de sinal de GPS
perc_conformidade_tempo		    INTEGER	    Razão do tempo aferido da viagem pelo planejado x 100
versao_modelo		            STRING	    Versão da metodologia de cálculo da respectiva linha na tabela
datetime_ultima_atualizacao     DATETIME	Última atualização [GMT-3]
*/