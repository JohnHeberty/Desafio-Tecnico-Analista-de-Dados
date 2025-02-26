from enum import Enum

class constants(Enum):
    """
    Valores constantes para msg de log
    """
    
    init__                            = {
        "Exception1":   "day_start deve ser do tipo datetime, mas recebeu {}",
        "Exception2":   "day_end deve ser do tipo datetime, mas recebeu {}"
    }
    
    checkPaths                        = {
        "Info":         "Arquivo {} localizado com sucesso, (__checkPaths);",
        "Error":        "Arquivo inexistente: {}, (__checkPaths);",
        "Exception":    "Existe arquivos inexistentes, verifique os logs, (__checkPaths);"
    }
    
    authenticate                      = {
        "Info":         "Autenticado com sucesso no bigquery, (__authenticate);",
        "Error":        "Erro na autenticacao com o bigquery, (__authenticate), {};",
        "Exception":    ""
    }
    
    saveCache                         = {
        "Info":         "Arquivo {} salvo com sucesso, (__saveCache);",
        "Error":        "Erro ao salvar o arquivo, (__saveCache), {};",
        "Exception":    ""
    }
     
    extract                           = {
        "Info1":         "Query {} carregada com sucesso, (__extract);",
        "Error1":        "Erro de leitura do arquivo, (__extract), {};",
        "Exception1":    "",
        
        "Info2":         "Parametro force ativado, reconsultando no BigQuery, (__extract);",
        "Error2":        "",
        "Exception2":    "",
                
        "Info3":         "Consulta no BigQuery realizada com sucesso, (__extract);",
        "Error3":        "Erro na consulta de dados do BigQuery, (__extract), {};",
        "Exception3":    "",
                        
        "Info4":         "Dados puxados do cache com sucesso, (__extract);",
        "Error4":        "Erro na leitura do aqruivo {}, (__extract), {};",
        "Exception4":    "",
        
    }
    
    transform_types                   = {
        "Info":         "Typagem de dados realizada com sucesso (__transform_types);",
        "Error":        "Erro na tipagem de dados (__transform_types), {};",
        "Exception":    ""
    }
    
    transform_choice_cols             = {
        "Info":         "Colunas renomeadas com sucesso (__transform_choice_cols);",
        "Error":        "Erro ao salvar renomear colunas do dataframe (__transform_choice_cols), {};",
        "Exception":    ""
    }
    
    transform_score_partida_chegada   = {
        "Info":         "Score das partidas e chegadas calculado com sucesso, (__transform_score_partida_chegada);",
        "Error":        "Erro ao calcular score das partidas e chegadas, (__transform_score_partida_chegada), {};",
        "Exception":    ""
    }
    
    transform_score_tempo_viagem      = {
        "Info1":         "Part 1 calculada com sucesso, (__transform_score_tempo_viagem);",
        "Error1":        "Erro na Part 1 do calculo de tempo viagem, (__transform_score_tempo_viagem), {};",
        "Exception1":    "",
        
        "Info2":         "Part 2 calculada com sucesso, (__transform_score_tempo_viagem);",
        "Error2":        "Erro na Part 2 do calculo de tempo viagem, (__transform_score_tempo_viagem), {};",
        "Exception2":    ""
    }
    
    transform_score_agregado          = {
        "Info":         "Agregação dos scores realizada com sucesso, (__transform_score_agregado);",
        "Error":        "Erro no processo de agregação dos scores, (__transform_score_agregado), {};",
        "Exception":    ""
    }
    
    transform_score_diversidade       = {
        "Info":         "Diversidade do score calculada com sucesso, (__transform_score_diversidade);",
        "Error":        "Erro ao calcular a diversidade so score, (__transform_score_diversidade), {};",
        "Exception":    ""
    }
    
    transform_score_normalizado       = {
        "Info":         "Normalização do score realizada com sucesso, (__transform_score_normalizado);",
        "Error":        "Erro na normalização do score, (__transform_score_normalizado), {};",
        "Exception":    ""
    }
    
    transform_nome_colunas            = {
        "Info":         "Renomeação das colunas realizada com sucesso, (__transform_nome_colunas);",
        "Error":        "Erro ao renomear as colunas, (__transform_nome_colunas), {};",
        "Exception":    ""
    }
    
    getRushHour                         = {
        "Info":         "Processamento horas picos realizada com sucesso, (getRushHour);",
        "Error":        "Erro ao processesar as horas picos, (getRushHour), {};",
        "Exception":    ""
    }
    
    getServiceRanking                   = {
        "Info":         "Processamento do ranking realizado com sucesso, (getServiceRanking);",
        "Error":        "Erro no processamento do ranking dos servicos, (getServiceRanking), {};",
        "Exception":    ""
    }
    
    getRanking10                        = {
        "Info":         "",
        "Error":        "Erro na solicitação dos top 10 melhores servicos, ranking vazio!, (getRanking10);",
        "Exception":    ""
    }
    
    getRankingBottom10                  = {
        "Info":         "",
        "Error":        "Erro na solicitação dos top 10 piores, ranking vazio!, (getRankingBottom10);",
        "Exception":    ""
    }
        
    getRankingHour                    = {
        "Info":         "Agrupamento do ranking por hora relizado com sucesso, (__getRankingHour);",
        "Error":        "Erro no agrupamento do ranking por hora, (__getRankingHour) {};",
        "Exception":    ""
    }
    
    getRankingMaxHour                   = {
        "Info":         "",
        "Error":        "Erro na solicitação dos melhores servicos por hora, resultado vazio!, (getRankingMaxHour);",
        "Exception":    ""
    }
        
    getRankingMinHour                   = {
        "Info":         "",
        "Error":        "Erro na solicitação dos piores servicos por hora, resultado vazio!, (getRankingMinHour);",
        "Exception":    ""
    }
    
    getRankingMaxRushHour               = {
        "Info":         "",
        "Error":        "Erro na solicitação dos melhores servicos por hora pico, resultado vazio!, (getRankingMaxRushHour);",
        "Exception":    ""
    }
        
    getRankingMinRushHour               = {
        "Info":         "",
        "Error":        "Erro na solicitação dos piores servicos por hora pico, resultado vazio!, (getRankingMinRushHour);",
        "Exception":    ""
    }
    
    makeSubfaixa15                    = {
        "Info":         "SubFaixas de 15 min criada com sucesso, (__makeSubfaixa15);",
        "Error":        "Erro na criação das SubFaixas de 15 min, (__makeSubfaixa15), {}",
        "Exception":    ""
    }
    
    getRankingMaxSubfaixa15             = {
        "Info":         "Solicitação do ranking dos melhores servicos por subfaixa horaria de 15 min feita com sucesso, (getRankingMaxSubfaixa15);",
        "Error":        "Erro na solicitação do ranking dos melhores servicos por subfaixa horaria de 15 min, (getRankingMaxSubfaixa15), {};",
        "Exception":    ""
    }
    
    getRankingMinSubfaixa15             = {
        "Info":         "Solicitação do ranking dos piores servicos por subfaixa horaria de 15 min feita com sucesso, (getRankingMinSubfaixa15);",
        "Error":        "Erro na solicitação do ranking dos piores servicos por subfaixa horaria de 15 min, (getRankingMinSubfaixa15), {};",
        "Exception":    ""
    }