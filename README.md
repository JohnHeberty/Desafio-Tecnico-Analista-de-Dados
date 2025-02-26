# Desafio Técnico – Analista de Dados
### Secretaria Municipal de Transportes
### Prefeitura da Cidade do Rio de Janeiro
### **Objetivo**

Este desafio avalia a capacidade do candidato em associar viagens planejadas (`GTFS`) com viagens realizadas no transporte público do Rio de Janeiro. A análise busca medir a aderência da operação ao planejamento, identificando padrões e propondo métricas para avaliação da qualidade do serviço.

O candidato deverá desenvolver um código em `Python e SQL` que realize essa associação para todas as viagens realizadas no mês de `dezembro de 2024`, seguindo critérios estabelecidos.

# LEGENDA
- Onde tiver o emoji 📓, significa que e o `resultado da analise` para item em questão.

# Descrição do Desafio

## <label><input type="checkbox" id="meuCheckbox1" checked></label> 1. Combinação das Tabelas Relevantes

### 🎯 **Objetivo**:

O candidato deve cruzar diferentes tabelas para estruturar a base de análise:

- `rj-smtr.gtfs.trips` – Contém informações sobre as viagens planejadas;
- `rj-smtr.gtfs.frequencies` – Define os horários de partida das viagens ao longo do dia;
- `rj-smtr.gtfs.routes` – Fornece informações sobre os serviços (linhas) de ônibus;
- `rj-smtr.planejamento.calendario` – Define os `service_ids` válidos para cada dia do mês;
- `rj-smtr.projeto_subsidio_sppo.viagem_completa` – Contém registros das viagens realizadas.

### 📓 **Realizado**

A estratégia adotada para esta análise foi a exploração inicial dos dados, uma vez que não estava claro quais parâmetros das tabelas deveriam ser utilizados para a união. Para essa etapa, foi executada uma query para cada tabela, com o objetivo de compreender sua estrutura de dados. Esse processo pode ser visualizado na pasta `/ExploratoryAnalysis`, localizada na raiz do projeto.

Nos itens disponíveis em `/ExploratoryAnalysis`, a análise foi conduzida com base na documentação oficial do `GTFS` (<a href="https://gtfs.org/documentation/schedule/reference/">gtfs.org</a>), a fim de compreender melhor as informações fornecidas por cada tabela. A partir dessa análise, foram estabelecidos os relacionamentos entre elas, identificando as colunas realmente relevantes para a estruturação da base de dados e futuras consultas.

Após a análise individual de cada tabela e a criação das respectivas queries, todas foram integradas utilizando o comando `WITH <table-name> AS (<sql-table>)`. Em seguida, foram aplicados os `JOINs` e filtros necessários para consolidar os dados.

A tabela final pode ser localizada em:

### 📓 **SQL Final**
```sh
./src/queries/Viagens/Planejadas/Planejadas.sql
```

Para otimizar o desempenho, foi utilizado o campo `feed_version` como um parâmetro de filtro em todas as tabelas, pois ele indica a `versão atual do conjunto de dados`. Como os dados possuem um versionamento, não é necessário processar todas as versões disponíveis, reduzindo assim a carga no `BigQuery`.

Além disso, foi realizada uma análise de desempenho, foi deixado o relato nas 10 primeiras linhas da consulta, avaliando o número de registros retornados e o tempo de execução. Esse processo foi iterativo até que se identificasse um valor de `feed_version` que não alterasse a quantidade de linhas retornadas, garantindo um equilíbrio entre precisão e eficiência computacional.

## <label><input type="checkbox" id="meuCheckbox2" checked></label> 2. Filtrar os Dados para Incluir Apenas Ônibus e Dias Válidos

### **Objetivo**:

- Manter apenas as viagens (`trips`) cujo `service_id` esteja presente na tabela `rj-smtr.planejamento.calendario`, garantindo que a viagem está prevista
para aquele dia;

- Filtrar apenas as linhas de ônibus, identificadas pelos `agency_ids` que representam os consórcios de ônibus `(Internorte, Intersul, Santa Cruz e Transcarioca)`.

### 📓 **Realizado**:

#### **Objetivo 1**
Nesta etapa, foi criada uma query para buscar os `service_ids` na tabela `rj-smtr.planejamento.calendario`, a fim de filtrar os dados na `tabela rj-smtr.gtfs.trips`. No entanto, logo no início, surgiu uma dificuldade: a coluna `service_id` na tabela `rj-smtr.planejamento.calendario` é um `array de strings`, enquanto na tabela `rj-smtr.gtfs.trips` é uma `string simples`. A solução proposta foi utilizar o comando `UNNEST()` para ajustar a `query` e resolver esse problema, fazendo com que a consulta retornasse cada valor separado em linhas diferentes. Isso fez duplicar as linhas do banco proporcionalmente a ao tamanho do array dentro de cada `service_ids`.

#### **Objetivo 2**
Para filtrar apenas as linhas de ônibus, foi necessário acessar os `agency_ids` que representam os consórcios de ônibus `(Internorte, Intersul, Santa Cruz e Transcarioca)`. A relação foi feita pela tabela `rj-smtr.gtfs.agency`, onde foi encontrado a coluna `agency_name` com o nome de cada `consórcio`, permitindo o filtro apenas nos que foram solicitados.

## <label><input type="checkbox" id="meuCheckbox3" checked></label> 3. Tratar os Horários de Início e Fim das Viagens (start_time e end_time)

### 🎯 **Objetivo**:

- Os horários na tabela `frequencies` estão no formato `HH:MM:SS`, mas podem ultrapassar `23:59:59`, indicando viagens que iniciam ou terminam no dia seguinte;
- ○ Exemplo: para o dia `2025-02-12`, um horário `25:00:00` corresponde a
`2025-02-13 01:00:00`;

- Ajustar esses horários para garantir que sejam corretamente interpretados no
contexto do dia da viagem.

### 📓 **Realizado**:
#### 📓 **SQL**: 
```SQL
            (
                data + 
                MAKE_INTERVAL( hour   => SAFE_CAST(SPLIT(start_time, ':')[OFFSET(0)] AS INT64)) +
                MAKE_INTERVAL( minute => SAFE_CAST(SPLIT(start_time, ':')[OFFSET(1)] AS INT64)) +
                MAKE_INTERVAL( second => SAFE_CAST(SPLIT(start_time, ':')[OFFSET(2)] AS INT64))
            ) as datetime_partida,
```
#### `Processo de cálculo`:  
- Primeiramente, utilize a função `SPLIT` para dividir o tempo nos separadores `:` e, em seguida, utilize `OFFSET` para selecionar a posição desejada (`0 a 2`), onde:  
  - `0` representa as **horas**  
  - `1` representa os **minutos**  
  - `2` representa os **segundos**  

- Posteriormente, passe o valor para a função `SAFE_CAST`, que garante a conversão do dado sem gerar erros.  

- Com os valores convertidos, utilize a função `MAKE_INTERVAL`, especificando o respectivo parâmetro: `hour`, `minute` ou `second`.  

- Por fim, some esse intervalo à data inicial, que parte de `00:00:00`.  

Para o cálculo de `datetime_chegada`, o processo é mais complexo, pois envolve a soma de `datetime_partida` com o **intervalo** entre `start_time` e `end_time`. Veja o exemplo abaixo.

#### 📓 **SQL**: 
```SQL
            (
            -- ############################ datetime_partida ####################################
                data + 
                MAKE_INTERVAL( hour   => SAFE_CAST(SPLIT(start_time, ':')[OFFSET(0)] AS INT64)) +
                MAKE_INTERVAL( minute => SAFE_CAST(SPLIT(start_time, ':')[OFFSET(1)] AS INT64)) +
                MAKE_INTERVAL( second => SAFE_CAST(SPLIT(start_time, ':')[OFFSET(2)] AS INT64))  
            -- ##################################################################################

                + -- MAIS

            -- ################### (end_time - start_time) = INTERVAL ###########################
                INTERVAL TIME_DIFF(
                    TIME(TIMESTAMP_SECONDS(
                            SAFE_CAST(SPLIT(end_time, ':')[OFFSET(0)] AS INT64) * 3600 +
                            SAFE_CAST(SPLIT(end_time, ':')[OFFSET(1)] AS INT64) * 60 +
                            SAFE_CAST(SPLIT(end_time, ':')[OFFSET(2)] AS INT64)
                        )
                    ),
                    TIME(TIMESTAMP_SECONDS(
                            SAFE_CAST(SPLIT(start_time, ':')[OFFSET(0)] AS INT64) * 3600 +
                            SAFE_CAST(SPLIT(start_time, ':')[OFFSET(1)] AS INT64) * 60 +
                            SAFE_CAST(SPLIT(start_time, ':')[OFFSET(2)] AS INT64)
                        )
                    ),
                    SECOND
                ) SECOND
            ) AS datetime_chegada
            -- ##################################################################################
```
#### `Processo de cálculo`:  
O cálculo de `datetime_partida` deve ser somado ao processo atual. Por isso, destacamos onde está localizado `datetime_partida`, pois o trecho em foco é a diferença `(end_time - start_time)`, que resulta em um **INTERVAL**.  

Tanto `start_time` quanto `end_time` passam pelo mesmo processo inicial de cálculo até o `SAFE_CAST`. A partir daí:  
- **Horas são convertidas para segundos** (`horas * 3600`).  
- **Minutos são convertidos para segundos** (`minutos * 60`).  
- **Segundos permanecem inalterados**.  
- **Todos os valores em segundos são somados**.  

O valor final é passado para a função `TIMESTAMP_SECONDS`, que converte os segundos (`INT64`) para um **TIMESTAMP**. Em seguida, esse `TIMESTAMP` é transformado em um **TIME** por meio da função `TIME`.  

Esse processo é repetido para `start_time` e `end_time`. Por fim, os valores resultantes são passados para `TIME_DIFF`, que calcula a diferença entre eles.

## <label><input type="checkbox" id="meuCheckbox4" checked></label> 4. Geração de Partidas das Viagens

### 🎯 **Objetivo** 
- Para cada `trip` da tabela `frequencies`, criar os horários de partida com base nas colunas `start_time`, `end_time` e `headway_secs` (intervalo entre partidas).

### 📓 **Realizado**

Nesta etapa, o foco foi **SQL**, devido ao seu desempenho significativamente superior ao **Python**. Como o objetivo é otimizar ao máximo a performance, toda a lógica foi implementada diretamente em **SQL**.  

A estratégia adotada para gerar viagens seguiu o seguinte passo a passo:  

- Primeiro, utilizamos a tabela resultante do **passo 3**, a **temp_Viagens_Planejadas**.  
- Foi preciso realizar a expansão proporcional à quantidade de viagens dentro do intervalo **datetime_inicio** e **datetime_fim**, seguindo o **headway**.  
- Assim, foi criada a **temp_EXP_Viagens_Planejadas**, utilizando a função **GENERATE_ARRAY** combinada com **UNNEST**.  
- O **GENERATE_ARRAY** gerou os intervalos da seguinte maneira: `tempo_viagem / headway = Número de viagens`.  
- Essa etapa garantiu a duplicação das linhas na quantidade exata correspondente ao **Número de viagens** para cada **trip_id**.  
- Com a estrutura pronta, foi criada a **GenerateTrips**, responsável por ajustar os valores de **datetime_inicio** e **datetime_fim** com base no `headway * Número da Viagem Atual`.  
- Em seguida, pegou-se a **GenerateTrips**, e agrupou-se por viagens duplicadas usando o **consorcio, servico_realizado, trip_id, shape_id, sentido** para gerar um **row_id** único para cada linha dentro do grupo, iniciando em **1** e incrementando até atingir o **Número de viagens**.  
- Por fim, a tabela **EndGenerateTrips** foi criada, contendo o **Id_Viagem**, permitindo realizar **JOINs** com outras tabelas.


## <label><input type="checkbox" id="meuCheckbox5" checked></label> 5. Associar Viagens Planejadas e Realizadas

### 🎯 **Objetivo** 

- A partir das viagens partidas geradas anteriormente, desdobrar as partidas e associá-las com as viagens registradas em `rj-smtr.projeto_subsidio_sppo.viagem_completa`, respeitando uma `tolerância máxima de 50% do intervalo entre partidas do mesmo serviço (linha) em viagens consecutivas`.

### 📓 **Realizado**

A solução para tarefa seguiu o seguinte passo a passo:

- Antes de tudo foi preciso padronizar as colunas em ambas tabelas, tanto em ordem quanto em nome;
- Foi ultilizado como chace de `JOIN` as sequintes colunas:
    - `feed_version`;
    - `feed_start_date`;
    - `consorcio`;
    - `vista`;
    - `trip_id`;
    - `sentido`;
    - `shape_id`;
    - `servico_realizado`;
    - `datetime_partida`;
    - `tempo_viagem`;
- O `JOIN` direto irá resultar em frames vazios, para resolver isso foi aplicado um intervalo limite:
    - Se o datetime_partida (Realizado) estiver dentro dos limites: 
        - datetime_partida (Planejado) + tempo_viagem (Planejado) * `FATOR DE UNIÃO`;
        - datetime_partida (Planejado) - tempo_viagem (Planejado) * `FATOR DE UNIÃO`;
    - O `FATOR DE UNIÃO` foi ajustado em `50%` (0.5), conforme manda o desafio.
- Após a união foi criado outro fator, o mesmo diz a repeito da `consecutividade`, que avalia se o indicador da viagem planejada está coerente com a viagem realizada:
    - viagens_consecutivas ( Planejado )   >= (viagens_consecutivas ( Realizado ) - `consecutive_factor`)
    - viagens_consecutivas ( Planejado )   <= (viagens_consecutivas ( Realizado ) + `consecutive_factor`)

Após a união seguindo esses criterios acima a query resultou em `33.451` assosiações.

-------------------------------------------------------------------------------------
## <label><input type="checkbox" id="meuCheckbox6" checked></label> 6. Utilizar o Feed Correto do GTFS
### 🎯 **Objetivo**:
- Identificar o feed correto para cada viagem utilizando os campos `feed_start_date` e `feed_end_date` das tabelas `rj-smtr.gtfs.feed_info` e `rj-smtr.projeto_subsidio_sppo.viagem_completa`.

### 📓 **Resultado da Análise**: 
- Os campos abaixos foram os que foi ultilizado para truncar a informação correta do `feed_start_date` e `feed_end_date`.
<table>
    <thead>
        <tr>
            <th>Viagem_Completa</th>
            <th>Operador</th>
            <th>Feed_Info</th>
            <th>Motivo</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <th><code>versao_modelo</code></th>
            <th><strong>=</strong></th>
            <th><code>versao_modelo</code></th>
            <th>Existe versionamento do GitHub, precisa escolher o correto.</th>
        </tr>
        <tr>
            <th><code>inicio_periodo</code></th>
            <th><strong>>=</strong></th>
            <th><code>feed_start_date</code></th>
            <th>O Início do período de operação planejado tem que estar depois ou iqual o periodo da versão correta</th>
        </tr>
        <tr>
            <th><code>fim_periodo</code></th>
            <th><strong><=</strong></th>
            <th><code>feed_end_date</code></th>
            <th>Fim do período de operação planejado tem que estar antes ou iqual ao fim do versionamento dos dados</th>
        </tr>
    </tbody>
</table>

- Se o `inicio_periodo` e `fim_periodo` estiverem dentro do `feed_start_date` e `feed_end_date`, significa que esses dados pertencem ao versionamento de dados presente no `feed_start_date` e `feed_end_date`.

## <label><input type="checkbox" id="meuCheckbox7"></label> 7. Tratamento de Erros e Dados Incompletos

### 🎯 **Objetivo**:

- O candidato deve sugerir abordagens para lidar com viagens sem correspondência exata.

### ❌ **NÃO Realizado**:

Este processo não pôde ser realizado, pois minha query provavelmente apresenta problemas de associação. Se não forem aplicados filtros para eliminar dados nulos, o retorno totaliza `1.086.788` linhas, mas apenas `33.451` dessas associações são bem-sucedidas.

Dessa forma, tentar generalizar mais de `25%` dos dados seria inviável. Isso implicaria uma modificação substancial na curva de distribuição da amostra, tornando-a artificial e possivelmente distorcendo padrões fundamentais do conjunto de dados. Se hoje a amostra exibe determinado comportamento, ao tentar preencher os dados nulos com base apenas em `33.451` registros, o comportamento pode ser drasticamente alterado, comprometendo a integridade da análise.

Caso o percentual de dados ausentes fosse menor que `25%`, poderíamos adotar abordagens estatísticas mais robustas, como o uso de modelos de aprendizado de máquina. Métodos como regressão, interpolação ou imputação baseada em clusters poderiam ser empregados para preencher as lacunas, desde que validados por técnicas como `k-fold cross-validation` para garantir a precisão e a confiabilidade das previsões`, para não levar indicadores inviesados para a diretoria.

## <label><input type="checkbox" id="meuCheckbox8" checked></label> 8. Remoção de Viagens Duplicadas
### 🎯 **Objetivo**:
- Garantir que cada viagem tenha um identificador único e eliminar registros redundantes.

### 📓 **Realizado**:
- De fato foi identificado linhas duplicadas. Para resolver esse problema, foi aplicado o comando `DISTINCT` na consulta, garantindo a remoção dos valores duplicados da coluna `trip_id` que representa um identificador unico da viagem. 

- Além de que foi usado, (`feed_version`, `feed_start_date`, `feed_end_date` e `versao_modelo`) em varias relações de `JOIN` para trazer os dados unicos, após isso foi encontrado um unico `trip_id` para cada viagem.

## <label><input type="checkbox" id="meuCheckbox9" checked></label> 9. Indicar uma Métrica de Avaliação

### 🎯 **Objetivo**:

-  O candidato deve propor um indicador para avaliar `quais serviços operam melhor (maior regularidade) e quais operam pior (menor regularidade)`;
- Deve-se levar em consideração a regularidade em diferentes faixas horárias `(a cada hora)` e `subfaixas horárias (a cada 15 minutos)`;
- Um serviço pode operar bem no `pico da manhã`, mas muito mal no `pico da tarde`, e isso deve ser levado em consideração na análise.

### 📓 **Realizado**:

Para a criação do indicador, foi imprescindível a leitura de um artigo científico publicado com foco na avaliação da qualidade do serviço público com base na percepção dos usuários. Segue a referência do trabalho de **Strehl et al.**:

- [Atributos Qualitativos e Fatores de Satisfação com o Transporte Público Urbano por Ônibus](https://www.redalyc.org/journal/5707/570761046004/html/#:~:text=A%20qualidade%20do%20transporte%20coletivo,caracter%C3%ADsticas%20dos%20locais%20de%20parada.)  

Em resumo, o artigo investiga os elementos que influenciam a satisfação dos usuários com os serviços de ônibus urbanos. O estudo avaliou a satisfação desses atributos em uma amostra de 203 usuários. Os resultados indicaram uma correlação significativa entre a satisfação geral e a intenção de continuar utilizando o serviço. Além disso, o artigo estabeleceu um ranking das preferências com base na importância declarada pelos usuários:

- **1º Segurança pública**
- **2º Rapidez**
- **3º Disponibilidade**
- **4º Segurança contra acidentes de trânsito**
- **5º Conforto dos ônibus**
- **6º Acesso**
- **7º Gasto**
- **8º Confiabilidade**
- **9º Conforto dos pontos de ônibus**
- **10º Ruído e poluição**
- **11º Informação**
- **12º Atendimento**
- **13º Conforto dos terminais**
- **14º Pagamento**
- **15º Integração**

Os itens `2º` e `8º` foram os possíveis de serem aplicados nesta avaliação. Para o **`2º (Rapidez)`**, temos a coluna `tempo_viagem`, e para o **`8º (Confiabilidade)`**, podemos trabalhar com a certeza da saída do ônibus e da chegada, com base nas colunas `datetime_partida (Planejada)`, `datetime_partida (Realizada)`, `datetime_chegada (Planejada)` e `datetime_chegada (Realizada)`.

- Com os indicadores escolhidos para avaliação, em algum momento será necessário somar os dois para chegar a um único valor. Para isso, a agregação dos dois seguirá o ranking de utilidade dos itens para o usuário, de modo que o peso de **rapidez** é **2/14** e o de **confiabilidade** é **8/14**.

Agora que os indicadores foram definidos, precisamos avaliá-los. Para isso, foi imprescindível a leitura do manual de [Indicadores de Qualidade no Transporte Público por Ônibus](https://issuu.com/proextfeb/docs/indicadores_de_qualidade_volume_1), pois ele contém diversas fórmulas detalhadas sobre como avaliar o transporte público.

Com isso, chegamos às duas tabelas abaixo para avaliação de qualidade:

---
| **Tempo de Viagem**                             | **Scores** | **Diferença (min)** | **Adaptado** |
|:-----------------------------------------------:|:----------:|:-------------------:|:------------:|
| Tempo médio de viagem maior que 80 minutos      | 0,00       | -                   |              |
| Tempo médio de viagem entre 50 e 80 minutos     | 0,25       | 30                  |              |
| Tempo médio de viagem entre 35 e 50 minutos     | 0,50       | 15                  |              |
| Tempo médio de viagem entre 20 e 35 minutos     | 0,75       | 15                  |              |
| Tempo médio de viagem entre 13 e 20 minutos     | 1,00       | 7                   |  X           |
| Tempo médio de viagem entre  7 e 13 minutos     | 1,25       | 7                   |  X           |
| Tempo médio menor que 7 minutos                 | 1,50       | -                   |  X           |
---

E também:

---
| **Confiabilidade (Pontuação ajustada)**                     | **Scores** | **Adaptado** |
|:-----------------------------------------------------------:|:----------:|:------------:|
| Mais de 12 min de adiantamento ou mais de 20 min de atraso  | 0,00       | X            |
| Máximo de 12 min de adiantamento ou 20 min de atraso        | 0,25       | X            |
| Máximo de 9 min de adiantamento ou 15 min de atraso         | 0,50       | X            |
| Máximo de 6 min de adiantamento ou 10 min de atraso         | 0,75       | X            |
| Máximo de 3 min de adiantamento ou 5 min de atraso          | 1,00       |              |
---

Nota-se que existe uma coluna **Adaptado**, criada para permitir a avaliação de um grupo de linhas e obter um resultado por companhia. Entretanto, para prosseguir com a análise, essa coluna foi ajustada para avaliar cada linha individualmente.

Dessa forma, foram desenvolvidas funções que utilizam intervalos específicos para calcular os scores, permitindo uma avaliação individual por linha.

Para responder às seguintes questões:

- Quais serviços operam melhor (maior regularidade)?
- Quais operam pior (menor regularidade)?
- Qual a regularidade em diferentes faixas horárias (a cada hora)?
- Qual a regularidade em subfaixas horárias (a cada 15 minutos)?

Primeiramente, realizou-se a mensuração dos serviços regulares. Para isso, foi criada a tabela `df_pesos`, que contém os pesos de cada serviço por faixa horária. Esse peso é calculado pela quantidade de viagens realizadas pelo serviço em relação ao total de viagens ocorridas na mesma faixa horária, demonstrando sua representatividade no total de operações.

Com a tabela `df_pesos` pronta, foi realizado um agrupamento na tabela `df_end`, que contém os valores dos indicadores. Esse agrupamento foi feito por `hora` e `serviço`, e, a partir dele, obteve-se a média do score atingido naquela faixa horária. O valor médio do `score` foi então multiplicado pela representatividade das viagens em relação ao total, utilizando os dados da tabela `df_pesos`. O resultado foi um `score` corrigido.

Com o `score` corrigido por regularidade, ordenaram-se os valores do maior para o menor para identificar os `serviços que operam melhor (maior regularidade)` e do menor para o maior para identificar os `serviços que operam pior (menor regularidade)`.

Esse processo foi repetido para a análise das `subfaixas de 15 minutos`, chegando-se às mesmas conclusões sobre os melhores e piores serviços por subfaixa horária. Assim, somaram-se os dois indicadores para obter um resultado final único. Por fim, foi aplicada uma normalização para melhor visualização dos dados.

## <label><input type="checkbox" id="meuCheckbox10" checked></label> 10. Gerar um Ranking Final de Serviços

### 🎯 **Objetivo**:

- O resultado final deve obrigatoriamente conter um `ranking por serviço para o mês`, que será um `output no formato CSV (UTF-8)`;
- O arquivo deve conter as seguintes colunas:

### Informações Sobre o Serviço Avaliado: 
<div>
    <table>
        <thead>
            <tr>
                <th>Campo</th>
                <th>Descrição</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>posicao</td>
                <td>Posição no ranking (1º lugar = melhor serviço)</td>
            </tr>
            <tr>
                <td>servico</td>
                <td>Identificador do serviço avaliado (ex: ‘006’)</td>
            </tr>
            <tr>
                <td>indicador</td>
                <td>Indicador de desempenho calculado pelo candidato</td>
            </tr>
        </tbody>
    </table>
</div>

- O ranking deve apresentar os serviços ordenados do melhor para o pior com base no `indicador desenvolvido pelo candidato`.

### 📓 **Realizado**:

O resultado pode ser localizado na raiz do projeto:
```sh
./Ranking.csv
```