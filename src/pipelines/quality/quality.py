# AUTOR: JOHN HEBERTY DE FREITAS
# EMAIL: john.7heberty@gmail.com

# Imports necessarios
from    datetime                            import datetime, timedelta
from    google.oauth2                       import service_account          #   Conexão com Google Cloud
from    google.cloud                        import bigquery
from    tqdm.notebook                       import tqdm
import  matplotlib.pyplot                   as plt
import  pandas_gbq                          as pg                           #   Conexão com BigQuery pelo Pandas
import  pandas                              as pd
import  logging
import  warnings
import  os

from    .constants                          import constants                #   Usado para msg de log 
from    .score                              import scores                   # type: ignore
from    addons.Config.Engine               import *

warnings.filterwarnings('ignore')

######################################################
##                                                  ##
##  ESSE DOCUMENTO FOI IDENTADO COM VARIOS ESPAÇOS  ##
##  PARA FACILITAR O ENTENDIMENTO DOS LEITORES,     ##      
##  CASO DESCIDAM USAR E INTERESSANTE FORMATAR!     ##
##                                                  ##
######################################################

######################################################
##  LEGENDA DO LOG LEVEL - PODE SER AJUSTADO .ENV   ##
##  LOG LEVEL 0: SEM ANOTAÇÕES DE LOGS              ##
##  LOG LEVEL 1: ANOTAÇÕES SOMENTE DOS ERROS        ##      
##  LOG LEVEL 2: TODO TIPO DE ANOTAÇÕES             ##
######################################################

# MELHORIAS ###########################################################################################################
#                                                                                                                     #
# * Falta implanatar day_start e day_end para interligar com sql direto;                                              #
# * Precisa incluir feed_version como um parametro de entrada e deve ser repassado para o sql;                        #
# * Precisa incliur parametro para receber a companhias em avaliação em forma de lista para ser repassado para sql;   #
# * Precisa incluir parametro de união de tempo de 50% definido no desafio, interessante deixar flexivel;             #
# * Precisa incluir o parametro de consecutividade, e crucial que tenha noção que seja unido linhas consevutivas;     #
# * Precisa incluir o Ranking Final;                                                                                  #
#                                                                                                                     #
# Pq isso já não foi implementado ? Simples, de proposito!! Objetivo e me contratarem rsrsrs                          #
# Além de que tem um easter eggs (CABULOSO e LOGICO) aqui no projeto, e não e o gandalf,                              #
# se eu for para a proxima etapa eu conto!                                                                            #
#                                                                                                                     #
#######################################################################################################################
class qualityService:
    ''' PIPELINE PARA AVALIAÇÃO DA QUALIDADE DOS SERVIÇOS 
        PRESTADO PELAS COMPANIHAS DE ONIBUS
    '''
    
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            # logging.StreamHandler(),  # Exibe logs no console
            logging.FileHandler(os.path.join("addons","TEMP","qualityService.log"))    # Salva logs em um arquivo
        ]
    )
    
    def __init__(self, 
        day_start:          datetime=datetime(2024, 12, 1), 
        day_end:            datetime=datetime(2024, 12, 31), 
        w_tempoViagem:      float=(2/14), 
        w_confiabilidade:   float=(8/14)
    )                                           -> None:
        
        # VALIDA DATAS DE ENTRADA
        if not isinstance(day_start, datetime):
            raise TypeError(constants.init__.value["Exception1"].format(type(day_start)))
        if not isinstance(day_end, datetime):
            raise TypeError(constants.init__.value["Exception2"].format(type(day_end)))
        
        # Cria um logger para a classe
        self.logger                         = logging.getLogger(__name__)
        
        # PESOS
        self.w_tempoViagem                  = w_tempoViagem
        self.w_confiabilidade               = w_confiabilidade
        
        # Assim pode rodar em Linux e no Windows sem BO de path.
        self.path_credentials               = os.path.join("addons",   "Credentials",  ACESS_BIGQUERY["CREDENTIALS_NAME"])
        self.path_sql                       = os.path.join("queries",   "Viagens",      "Planejada_U_Realizada", "Merge.sql")
        self.path_csv_cache                 = os.path.join("addons",   "TEMP",         "CacheMerge.csv")
        
        # Usado para validar se existem 
        self.paths_check = [
            self.path_credentials,
            self.path_sql,
        ]
    
    def __checkPaths(self)                      -> None:
        ''' Objetivo em validar se arquivos existem
            caso não existam, realiza logs e para a
            execução    
        '''
        
        status_erro = False
        for path in self.paths_check:
            
            if not os.path.exists(path):
                
                if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
                    self.logger.error(constants.checkPaths.value["Error"].format(path))
                status_erro = True
                
            else:
                
                # CRIA A PASTA ATÉ O FILE DESEJADO CASO NÃO EXISTA
                os.makedirs(path.replace(os.path.basename(path),""), exist_ok=True)
                
                if LOG_CONFIG["LOG_LEVEL"] in [2]:
                    self.logger.info(constants.checkPaths.value["Info"].format(path))
                
        if status_erro:
            raise Exception(constants.checkPaths.value["Exception"])
        
    def __authenticate(self)                    -> None:
        ''' Realiza autenticacao no BigQuery
            sem esse passo não e possivel seguir.
        '''
        
        self.__checkPaths()
        
        #Lendo a chave de acesso da conta do serviço
        try:
            
            self.credentials                    = service_account.Credentials.from_service_account_file(self.path_credentials)
            
            if LOG_CONFIG["LOG_LEVEL"] in [2]:
                self.logger.info(constants.authenticate.value["Info"])
       
        except Exception as e:
            
            msg                                 = constants.authenticate.value["Error"].format(e)
            if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
                self.logger.critical(msg)
                
            raise Exception(msg)

    def __saveCache(self, df, path=None)       -> None:
        ''' Usada para salvar arquivos 
        '''
        
        # SALVANDO EM CACHE
        try:
            
            path_save                           = self.path_csv_cache if path is None else path 
            df.to_csv(path_save, encoding='utf-8', index=False, sep=';')
            
            if LOG_CONFIG["LOG_LEVEL"] in [2]:
                self.logger.info(constants.saveCache.value["Info"].format(path_save))
        
        except Exception as e:
            
            if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
                self.logger.error(constants.saveCache.value["Error"].format(path_save))
        
    def __extract(self, force:bool=False)       -> None:
        ''' Processo de Extração de Dados do BigQuery

        Args:
            force (bool, opcional): Se for ativado força a buscar os dados novamente. Padrão é False.
        '''
        
        # ABRINDO ARQUIVO DE SQL PARA CONSULTAR NO BANCO
        try:
            
            with open(self.path_sql, 'r') as file:
                query = file.read()
                
            if LOG_CONFIG["LOG_LEVEL"] in [2]:
                self.logger.info(constants.extract.value["Info1"].format(self.path_sql))
        
        except Exception as e:
            
            if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
                self.logger.error(constants.extract.value["Error1"].format(e))
        
        # SO PASSA SE ARQUVO EXISTIR OU FORCE FOR ATIVADO
        if os.path.exists(self.path_csv_cache) is False or force:
            
            if force: self.logger.info(constants.extract.value["Info2"])
            
            # BUSCANDO DADOS NO BIGQUERY
            try:
                
                self.df                         = pg.read_gbq(query,project_id=ACESS_BIGQUERY["PROJECT_NAME"], credentials=self.credentials)
                
                if LOG_CONFIG["LOG_LEVEL"] in [2]:
                    self.logger.info(constants.extract.value["Info3"])
            
            except Exception as e:
                
                if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
                    self.logger.error(constants.extract.value["Error3"].format(e))
            
            self.__saveCache(self.df)
        
        # LENDO DO CACHE
        else:
            
            try:
                
                self.df                         = pd.read_csv(self.path_csv_cache, sep=';', low_memory=True)
                
                if LOG_CONFIG["LOG_LEVEL"] in [2]:
                    self.logger.info(constants.extract.value["info4"])
            
            except Exception as e:
                
                if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
                    self.logger.error(constants.extract.value["Error4"].format(self.path_csv_cache, e))
            
    def __transform_types(self)                 -> None:
        ''' Processo de conversão de type

            Correções de types aceleram analises
            em grande quantidade de dados !
            
            Porque quardar um parafuso em uma 
            bacia ? E mesmo sentido se colocar 
            inteiro como variavel float, além 
            do computador  gastar  mais  tempo 
            procurando o parafuso garla todo o 
            processo adiante.... 
        '''
        
        # MAS RECOMENDAVEL MUDAR PARA PUXAR TYPES DE UM DICT E REALIZAR UM FOR PARA TRATAR
        try:
            
            self.df['servico_VP']                = self.df['servico_VP'].astype("string")
            self.df['service_VR']                = self.df['service_VR'].astype("string")
            self.df['datetime_partida_VP']       = pd.to_datetime(self.df['datetime_partida_VP'], format='%Y-%m-%d %H:%M:%S')
            self.df['datetime_partida_VR']       = pd.to_datetime(self.df['datetime_partida_VR'], format='%Y-%m-%d %H:%M:%S')
            self.df['datetime_chegada_VP']       = pd.to_datetime(self.df['datetime_chegada_VP'], format='%Y-%m-%d %H:%M:%S')
            self.df['datetime_chegada_VR']       = pd.to_datetime(self.df['datetime_chegada_VR'], format='%Y-%m-%d %H:%M:%S')
            self.df['tempo_viagem_VP']           = self.df['tempo_viagem_VP'].convert_dtypes(int)
            self.df['tempo_viagem_VR']           = self.df['tempo_viagem_VR'].convert_dtypes(int)
            
            if LOG_CONFIG["LOG_LEVEL"] in [2]:
                self.logger.info(constants.transform_types.value["Info"])
        
        except Exception as e:
            
            if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
                self.logger.error(constants.transform_types.value["Error"].format(e))
        
    def __transform_choice_cols(self)           -> None:
        ''' Escolhendo as coluna necessarias
        '''
        
        cols_choice = [
            "servico_VP",
            "datetime_partida_VP",
            "datetime_partida_VR",
            "datetime_chegada_VP",
            "datetime_chegada_VR",
            "tempo_viagem_VP",
            "tempo_viagem_VR"
        ]
        
        rename_cols = {
            "servico_VP":           "servico", 
            "datetime_partida_VP":  "partida_planejada", 
            "datetime_partida_VR":  "partida_realizada",
            "datetime_chegada_VP":  "chegada_planejada",
            "datetime_chegada_VR":  "chegada_realizada",
            "tempo_viagem_VP":      "tempo_viagem_planejada",
            "tempo_viagem_VR":      "tempo_viagem_realizada",
        }
        
        # RENOMEANDO COLUNAS
        try:
            
            self.df_ok                          = (self.df[cols_choice].copy()).rename(columns=rename_cols)
            
            if LOG_CONFIG["LOG_LEVEL"] in [2]:
                self.logger.info(constants.transform_choice_cols.value["Info"])
        
        except Exception as e:
            
            if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
                self.logger.error(constants.transform_choice_cols.value["Error"].format(e))
        
    def __transform_score_partida_chegada(self) -> None:
        ''' Calculando Indicador de partida e chegada
        '''
        
        try:
            
            self.df_ok["score_partida"]         = (self.df_ok["partida_realizada"] - self.df_ok["partida_planejada"]).apply(lambda x: scores.confiabilidade(x.total_seconds()) )
            self.df_ok["score_chegada"]         = (self.df_ok["chegada_realizada"] - self.df_ok["chegada_planejada"]).apply(lambda x: scores.confiabilidade(x.total_seconds()) )
            self.df_ok["score_partida_chegada"] = self.df_ok["score_partida"] + self.df_ok["score_chegada"]
            self.df_ok.drop(["score_partida", "score_chegada"], axis=1, inplace=True)
            
            if LOG_CONFIG["LOG_LEVEL"] in [2]:
                self.logger.info(constants.transform_score_partida_chegada.value["Info"])
        
        except Exception as e:
            
            if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
                self.logger.error(constants.transform_score_partida_chegada.value["Error"].format(e))
            
    def __transform_score_tempo_viagem(self)    -> None:
        ''' Calculando Indicador de tempo de viagem
        '''
        
        # PART 1
        try:
            
            self.df_ok[
                "score_viagem_planejada"]           =  self.df_ok["tempo_viagem_planejada"].apply(lambda x: scores.tempo_viagem(x) )
            self.df_ok[
                "score_viagem_realizada"]           =  self.df_ok["tempo_viagem_realizada"].apply(lambda x: scores.tempo_viagem(x) )
            
            if LOG_CONFIG["LOG_LEVEL"] in [2]:
                self.logger.info(constants.transform_score_tempo_viagem.value["Info1"])
        
        except Exception as e:
            
            if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
                self.logger.error(constants.transform_score_tempo_viagem.value["Error1"].format(e))
            
        # PART 2
        try:
            
            # SUBTRAI O SCORE REALIZADO PELO PLANEJADO
            # SE FICAR POSITIVO SIGNIFICA QUE REALIZADO FOI MELHOR QUE PLANEJAMENTO 
            # CASO CONTRARIO REALIZADO E PIOR QUE PLANEJAMENTO.
            self.df_ok["score_viagem"]              =   (self.df_ok["score_viagem_realizada"] - self.df_ok["score_viagem_planejada"])
            self.df_ok.drop(
                [
                    "score_viagem_planejada",
                    "score_viagem_realizada"
                ], axis=1, inplace=True)
            self.df_ok["score_viagem"]              =   self.df_ok["score_viagem"].replace(0, 1)  # CASO ZERAR SIGNIFICA QUE ATINGIU 100% DE PRESCISÃO!
           
            if LOG_CONFIG["LOG_LEVEL"] in [2]:
                self.logger.info(constants.transform_score_tempo_viagem.value["Info2"])
        
        except Exception as e:
            
            if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
                self.logger.error(constants.transform_score_tempo_viagem.value["Error2"].format(e))
    
    def __transform_score_agregado(self)        -> None:
        ''' Agrega os indicadores com base na ultilidade de cada um 
            repassada ao instanciar a classe.
        '''
        
        # APLICANDO PESOS NO SCORE E AGREDANDO
        try:
            
            self.df_ok["SCORE"]                 = (self.df_ok["score_partida_chegada"] * self.w_confiabilidade) + (self.df_ok["score_viagem"] * self.w_tempoViagem)
            self.df_ok.sort_values(
                ["SCORE"], 
                ignore_index=True, 
                ascending=False, 
                inplace=True
            )
            
            if LOG_CONFIG["LOG_LEVEL"] in [2]:
                self.logger.info(constants.transform_score_agregado.value["Info"])
       
        except Exception as e:
            
            if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
                self.logger.error(constants.transform_score_agregado.value["Error"].format(e))
    
    def __transform_score_diversidade(self)     -> None:
        ''' Processo realizado para aumentar a diversidade
            do indicador agregrado
            
            Fator Erro = FErro
        '''
        
        # SCORE se repete bastante, aqui aumenta sua diversidade
        try:
            
            Error                               = abs(self.df_ok["tempo_viagem_realizada"] - self.df_ok["tempo_viagem_planejada"])
            Total                               = self.df_ok["tempo_viagem_realizada"] + self.df_ok["tempo_viagem_planejada"]
            self.df_ok["FErro"]                 = 1 - (Error/Total)
            self.df_ok["SCORE"]                 = self.df_ok["SCORE"] * self.df_ok["FErro"]
            self.df_ok.drop(["FErro"], axis=1, inplace=True)
            self.df_ok.sort_values(
                ["SCORE"], 
                ignore_index=True, 
                ascending=False, 
                inplace=True
            )
            
            if LOG_CONFIG["LOG_LEVEL"] in [2]:
                self.logger.info(constants.transform_score_diversidade.value["Info"])
            
        except Exception as e:
            
            if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
                self.logger.error(constants.transform_score_diversidade.value["Error"].format(e))
        
    def __transform_score_normalizado(self)     -> None:
        ''' Existe dados negativos, com normalização some.
        '''
        
        # NORMALIZANDO DADOS
        try:
        
            SCORE_MAX                           = self.df_ok.SCORE.max()
            SCORE_MIN                           = self.df_ok.SCORE.min()
            self.df_ok.SCORE                    = (self.df_ok.SCORE - SCORE_MIN) / (SCORE_MAX - SCORE_MIN)
            
            if LOG_CONFIG["LOG_LEVEL"] in [2]:
                self.logger.info(constants.transform_score_normalizado.value["Info"])
        
        except Exception as e:
            
            if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
                self.logger.error(constants.transform_score_normalizado.value["Error"].format(e))
        
    def __transform_nome_colunas(self)          -> None:
        ''' Renomeando colunas para melhor entendimento
        '''
        
        # RENOMEANDO COLUNAS
        try:
            
            self.df_end                         = self.df_ok[["servico","SCORE","partida_realizada"]].copy()
            self.df_end.rename(columns          ={
                "partida_realizada": "data_partida",
                "SCORE":             "score",
            }, inplace=True)
            
            if LOG_CONFIG["LOG_LEVEL"] in [2]:
                self.logger.info(constants.transform_nome_colunas.value["Info"])
        
        except Exception as e:
            
            if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
                self.logger.error(constants.transform_nome_colunas.value["Error"].format(e))
    
    def run(self, verbose:bool=False)           -> None:
        ''' Processo de execução da pipeline sequenciado

        Args:
            verbose (bool, opcional): Se ativado mostra barra de carregamento. Padrao e False.
        '''
        # ESSA DEF NÃO PRECISA DE LOG!, JÁ REALIZADO ANTERIORMENTE.        
        
        # Pegando as credenciais de acesso
        self.__authenticate()
        
        # Valida se precisa de ver processamento carregando 
        if verbose: barra = tqdm(range(9), desc="Processing")

                                                    # Se Precisar de barra de processamento
        self.__extract(),                           barra.update(1) if verbose else None
        self.__transform_types(),                   barra.update(1) if verbose else None
        self.__transform_choice_cols(),             barra.update(1) if verbose else None
        self.__transform_score_partida_chegada(),   barra.update(1) if verbose else None
        self.__transform_score_tempo_viagem(),      barra.update(1) if verbose else None
        self.__transform_score_agregado(),          barra.update(1) if verbose else None
        self.__transform_score_diversidade(),       barra.update(1) if verbose else None
        self.__transform_score_normalizado(),       barra.update(1) if verbose else None
        self.__transform_nome_colunas(),            barra.update(1) if verbose else None
        if verbose: barra.close()
    
    def getRushHour(self, top_plot:float=0.75, f_desvio:float=0.5)  -> pd.DataFrame: 
        ''' Aplicando inferencia basica para identificar hora pico

        Args:
            top_plot (float, opcional): BoxPlot usa 0.75, e interessante rodar 
                                        para  outros  meses  e ver se a função 
                                        mantem  acertando,  caso  não  deve-se 
                                        ajustar.                Padrão é 0.75.
                        
            f_desvio (float, opcional): fator de desvio padrão, 1 e 50% da 
                                        amostra, 1.5 e 75% e 2 e 100% da
                                        amostra. Fator ajustado para 0.5
                                        pois a hora pico deve-se ajustar 
                                        abaixo de 25% dos dados. Padrão é 0.50.
                        
        Returns:
            pd.DataFrame: DataFrame[ Hora, Viagens ]
        '''
        
        # OBTENDO A HORA DO RUSH!
        try:
        
            # Realizadno a contagem de viagens por faixa horaria
            viagens_faixa_horaria               = self.df_end.data_partida.apply(lambda x: x.hour).value_counts().sort_index().to_frame().reset_index(drop=False)
            viagens_faixa_horaria.columns       = ['Hora', 'Viagens']
            
            # Certifique-se de que 'Hora' é do tipo string ou datetime
            viagens_faixa_horaria['Hora']       = viagens_faixa_horaria['Hora'].astype(int)
            
            viagens         = viagens_faixa_horaria.Viagens.copy() 
            madrugada_manha = viagens.iloc[:12]
            tarde_noite     = viagens.iloc[12:]

            # ANALISE ESTATISTICA PARA PUXAR OUTLIER, NESSE CASO TEMOS AS HORAS PICO!!!!
            vcut_m          = madrugada_manha[madrugada_manha    >= viagens.mean() * 0.75] # boxplot usa topo como 75%, outliers estão em menos de 25% dos dados
            vcut_t          = tarde_noite    [tarde_noite        >= viagens.mean() * 0.75] # boxplot usa topo como 75%, outliers estão em menos de 25% dos dados

            # RESPONSAVEL POR REMOVER HORAS PICO FALSO POSITIVO,
            # DEIXA HORA PICO SO SE TIVER MAIS 2H DE DISTANCIA, 
            # CASO FOR SEQUENCIADA IRÁ ESCOLHER A MAIOR
            def choicerushrange(rushhour):

                rushhour["DIF_H1"]  = abs(rushhour.Hora - rushhour.Hora.shift(1).fillna(0))
                rushhour["DIF_H-1"] = abs(rushhour.Hora - rushhour.Hora.shift(-1).fillna(0))

                # SUBTRAINDO PARA CIMA E BAIXO E VENDO A DIFERENÇA HORARIA, SE FOR IGUAL AGRUP E PEGUE MAIOR!
                min_save = []
                for _, row in rushhour.iterrows(): min_save.append(min([row["DIF_H1"], row["DIF_H-1"]]))
                rushhour.drop(["DIF_H1","DIF_H-1"], axis=1, inplace=True)
                rushhour["GRUP"]    = pd.Series(min_save, index=rushhour.index)
                rushhour            = rushhour.loc[rushhour.groupby("GRUP")["Viagens"].idxmax()]
                rushhour.drop(["GRUP"], axis=1, inplace=True)
                return rushhour
            
            # RUSH DA MANHÃ
            RUSH1           = choicerushrange(viagens_faixa_horaria.loc[vcut_m[vcut_m >= vcut_m.mean() + vcut_m.std() * 0.5].index]) # 0.5 POIS HORA PICO PRESENTA MENOS DE 25% DOS DADOS, DISTRIBUIÇÃO QUASE NORMAL!
            # RUSH DA TARDE
            RUSH2           = choicerushrange(viagens_faixa_horaria.loc[vcut_t[vcut_t >= vcut_t.mean() + vcut_t.std() * 0.5].index]) # 0.5 POIS HORA PICO PRESENTA MENOS DE 25% DOS DADOS, DISTRIBUIÇÃO QUASE NORMAL!
            
            # UNINDO TUDO.
            HORA_DO_RUSH    = pd.concat([RUSH2, RUSH1], ignore_index=True).sort_values(['Hora'], ignore_index=True)
            
            if LOG_CONFIG["LOG_LEVEL"] in [2]:
                self.logger.info(constants.getRushHour.value["Info"])
            
            return HORA_DO_RUSH

        except Exception as e:
            
            if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
                self.logger.error(constants.getRushHour.value["Error"].format(e))
            
            return pd.DataFrame(columns=["Hora","Viagens"])
        
    def getServiceRanking(self)                 -> pd.DataFrame:
        ''' Realiza a confeccao do ranking dos servicos

        Returns:
            pd.DataFrame: DataFrame[ posicao, servico, indicador ].
        '''
        
        # OBTENDO RANKING DOS SERVIÇOS
        try:
            
            # AGRUPA POR SERVICO E PUXA VALORES MEDIOS DO SCORE PARA CADA SERVIÇO
            ranking_services                    = self.df_end.groupby(by="servico").score.mean().to_frame().reset_index(drop=False).copy()
            ranking_services                    = ranking_services.sort_values('score', ignore_index=True, ascending=False).reset_index(drop=False)
            ranking_services.rename(columns     ={
                "index": "posicao",
                "score": "indicador",
            }, inplace=True)
            ranking_services.posicao            = ranking_services.posicao + 1
            
            if LOG_CONFIG["LOG_LEVEL"] in [2]:
                self.logger.info(constants.getServiceRanking.value["Info"])
            
            return ranking_services

        except Exception as e:
            
            if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
                self.logger.error(constants.getServiceRanking.value["Error"].format(e))
            
            return pd.DataFrame(columns=["posicao","servico","indicador"])
    
    def getScoreService(self, servico:str):
        ''' Puxa o score so servico desejado

        Args:
            servico (string): servico/linha a ser solicitado o score
        '''
        
        result                              = self.getServiceRanking()
        if not result.empty:                
            filtrado = result[result.servico.apply(str.strip) == str(servico).strip()]
            if not filtrado.empty:
                return filtrado.iloc[0].get("indicador", default=0)

        return -1
        
    def getRanking10(self)                      -> pd.DataFrame:
        ''' Busca-se um resultado mais simples
            somente os 10 primeiros do ranking

        Returns:
            pd.DataFrame: DataFrame[ posicao, servico, indicador ].
        '''
        
        result                              = self.getServiceRanking()
        if not result.empty:                return result.head(n=10)
        
        if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
            self.logger.error(constants.getRanking10.value["Error"])
        
        return pd.DataFrame(columns=["posicao","servico","indicador"])
    
    def getRankingBottom10(self)                -> pd.DataFrame:
        ''' Busca-se um resultado mais simples
            somente os 10 ultimos do ranking

        Returns:
            pd.DataFrame: DataFrame[ posicao, servico, indicador ].
        '''
        
        result                              = self.getServiceRanking()
        if not result.empty:                return result.iloc[::-1].head(n=10)
        
        if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
            self.logger.error(constants.getRankingBottom10.value["Error"])
        
        return pd.DataFrame(columns=["posicao","servico","indicador"])
    
    def __getRankingHour(self)                  -> tuple:
        ''' Realiza o agrupamento dos servicos
            para  ser  conceccionado o ranking 
            por faixa horaria

        Returns:
            Tupla: ( DataFrame.groupby.hour, DataFrame[ servico, score, data_partida, hora ] )
        '''
        
        try:
            
            self.df_end["hora"]             = self.df_end["data_partida"].apply(lambda x: x.hour)
            
            # Realizadno a contagem de viagens de serviço por faixa horaria
            ranking_services_regular        = self.df_end.groupby(by=["hora","servico"]).score.mean().to_frame().copy()

            # saindo do multiindex do grupby para frame
            ranking_services_regular        = ranking_services_regular.sort_values(['score'], ascending=False).reset_index(drop=False)
            
            if LOG_CONFIG["LOG_LEVEL"] in [2]:
                self.logger.info(constants.getRankingHour.value["Info"])
            
            return ranking_services_regular.groupby(by=["hora"]), ranking_services_regular
        
        except Exception as e:
            
            if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
                self.logger.error(constants.getRankingHour.value["Error"].format(e))
            
            return pd.DataFrame(columns=["servico","score","data_partida","hora"]).groupby
    
    def getRankingMaxHour(self)                 -> pd.DataFrame:
        ''' Criando Ranking dos melhores servicos 
            por faixa horaria

        Returns:
            pd.DataFrame: Dataframe[ hora, servico, score ]
        '''
    
        group, result                       = self.__getRankingHour()
        if not result.empty:                return result.loc[group["score"].idxmax()].reset_index(drop=True)
        
    
        if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
            self.logger.error(constants.getRankingMaxHour.value["Error"])
        
        return pd.DataFrame(columns=["hora","servico","score"])
        
    def getRankingMinHour(self)                 -> pd.DataFrame:
        ''' Criando Ranking dos piores servicos 
            por faixa horaria

        Returns:
            pd.DataFrame: Dataframe[ hora, servico, score ]
        '''
        
        group, result                       = self.__getRankingHour()
        if not result.empty:                return result.loc[group["score"].idxmin()].reset_index(drop=True)
    
        if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
            self.logger.error(constants.getRankingMinHour.value["Error"])
        
        return pd.DataFrame(columns=["hora","servico","score"])
    
    def getRankingMaxRushHour(self)             -> pd.DataFrame:
        ''' Criando Ranking dos melhores servicos
            por hora pico.

        Returns:
            pd.DataFrame: Dataframe[ hora, servico, score ]
        '''
        
        RushHour                            = self.getRushHour()
        if not RushHour.empty:
            RushHour.Hora                   = RushHour.Hora.astype(int)
            RankingMaxHour                  = self.getRankingMaxHour()
            RankingMaxHour.set_index(
                "hora", 
                inplace=True
            )
            RankingMaxHour                  = RankingMaxHour.loc[RushHour.Hora.values]
            RankingMaxHour.reset_index(
                drop=False, 
                inplace=True
            )
            return RankingMaxHour
        
        if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
            self.logger.error(constants.getRankingMaxRushHour.value["Error"])
        
        return pd.DataFrame(columns=["hora","servico","score"])
            
    def getRankingMinRushHour(self)             -> pd.DataFrame:
        ''' Criando Ranking dos piores servicos
            por hora pico.

        Returns:
            pd.DataFrame: Dataframe[ hora, servico, score ]
        '''
        
        RushHour                            = self.getRushHour()
        if not RushHour.empty:
            RushHour.Hora                       = RushHour.Hora.astype(int)
            RankingMinHour                      = self.getRankingMinHour()
            RankingMinHour.set_index("hora", inplace=True)
            RankingMinHour                      = RankingMinHour.loc[RushHour.Hora.values]
            RankingMinHour.reset_index(drop=False, inplace=True)
            return RankingMinHour
        
        if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
            self.logger.error(constants.getRankingMinRushHour.value["Error"])
        
        return pd.DataFrame(columns=["hora","servico","score"])
    
    def __makeSubfaixa15(self)                  -> None:
        ''' Criando as subfaixas de 15 em 15 min
            para servir de baseb de agrupamento 
            para aferir os melhores e piores 
            servicos por subfaixa horaria. 

        Returns:
            Add column in df_end, Dataframe[ **, SubFaixa15 ]
        '''
        
        try:
            
            # CASO RODE NOVAMENTE EVITA CRIAR COLUNAS EM EXESSO
            if "SubFaixa15" in self.df_end.columns:
                self.df_end.drop(columns=["SubFaixa15"], inplace=True)

            # Criar faixas de horário
            subfaixas_15min                     = pd.date_range(start="00:00", end="23:45", freq="15min").time
            faixas_horarias                     = pd.DataFrame({"Inicio": subfaixas_15min})
            faixas_horarias["Fim"]              = faixas_horarias["Inicio"].shift(-1, fill_value=subfaixas_15min[0])
            faixas_horarias["SubFaixa15"]       = faixas_horarias["Inicio"].astype(str) + " - " + faixas_horarias["Fim"].astype(str)

            # Converter para datetime para facilitar a comparação
            faixas_horarias["Inicio"]           = pd.to_datetime(faixas_horarias["Inicio"], format='%H:%M:%S').dt.time
            faixas_horarias["Fim"]              = pd.to_datetime(faixas_horarias["Fim"], format='%H:%M:%S').dt.time
            faixas_horarias.reset_index(inplace=True)

            # Converter datetime.time para minutos desde a meia-noite
            def time_to_minutes(t): return t.hour * 60 + t.minute
            faixas_horarias["Inicio_min"]       = faixas_horarias["Inicio"].map(time_to_minutes)
            faixas_horarias["Fim_min"]          = faixas_horarias["Fim"].map(time_to_minutes)

            # Criar um índice categórico baseado em hora
            self.df_end["HoraCompleta"]         = self.df_end["data_partida"].dt.time
            self.df_end["HoraMin"]              = self.df_end["HoraCompleta"].map(time_to_minutes)

            # Criar bins para segmentação eficiente
            bins                                = list(faixas_horarias["Inicio_min"]) + [time_to_minutes(pd.to_datetime(['23:59:00'], format='%H:%M:%S').time[0]) + 1] # Ultimo minuto do dia
            labels                              = faixas_horarias["SubFaixa15"]

            self.df_end["SubFaixa15"]           = pd.cut(
                self.df_end["HoraMin"], bins=bins, labels=labels, include_lowest=True, right=False
            )

            self.df_end.drop(columns=["HoraCompleta", "HoraMin"], inplace=True)
            self.df_end.SubFaixa15              = self.df_end.SubFaixa15.astype(str)
        
            if LOG_CONFIG["LOG_LEVEL"] in [2]:
                self.logger.info(constants.makeSubfaixa15.value["Info"])
            
        except Exception as e:
            
            if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
                self.logger.error(constants.makeSubfaixa15.value["Error"].format(e))
        
    def getRankingMaxSubfaixa15(self)           -> pd.DataFrame:
        ''' Criando um ranking dos melhores servicos 
            por subfaixa horaria de 15 min. 

        Returns:
            pd.DataFrame: Dataframe[ SubFaixa15, servico, score ]
        '''
        
        try:
            
            self.__makeSubfaixa15()
            # Realizadno a media do score do grupo de viagens feita pelo serviço dentro da faixa horaria 
            ranking_services_regular            = self.df_end.loc[self.df_end.groupby(by=["SubFaixa15"])["score"].idxmax()]
            ranking_services_regular            = ranking_services_regular.reset_index(drop=True).sort_values("score", ignore_index=True, ascending=False)
            
            if LOG_CONFIG["LOG_LEVEL"] in [2]:
                self.logger.info(constants.getRankingMaxSubfaixa15.value["Info"])
            
            return ranking_services_regular
        
        except Exception as e:
            
            if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
                self.logger.error(constants.getRankingMaxSubfaixa15.value["Error"].format(e))

            return pd.DataFrame(columns=["SubFaixa15","servico","score"])
        
    def getRankingMinSubfaixa15(self)           -> pd.DataFrame:
        ''' Criando um ranking dos piores servicos 
            por subfaixa horaria de 15 min. 

        Returns:
            pd.DataFrame: Dataframe[ SubFaixa15, servico, score ]
        '''
        
        try:
        
            self.__makeSubfaixa15()
            # Realizadno a media do score do grupo de viagens feita pelo serviço dentro da faixa horaria 
            ranking_services_regular            = self.df_end.loc[self.df_end.groupby(by=["SubFaixa15"])["score"].idxmin()]
            ranking_services_regular            = ranking_services_regular.reset_index(drop=True).sort_values("score", ignore_index=True, ascending=True)
            
            if LOG_CONFIG["LOG_LEVEL"] in [2]:
                self.logger.info(constants.getRankingMinSubfaixa15.value["Info"])
            
            return ranking_services_regular
        
        except Exception as e:
            
            if LOG_CONFIG["LOG_LEVEL"] in [1,2]:
                self.logger.error(constants.getRankingMinSubfaixa15.value["Error"].format(e))
        
            return pd.DataFrame(columns=["SubFaixa15","servico","score"])
    