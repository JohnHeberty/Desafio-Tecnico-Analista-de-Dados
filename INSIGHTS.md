# POSSÍVEIS MELHORIAS 🚀
---

## 1. Otimização de Colunas na Tabela `viagem_completa` 🔧
**Colunas**: (id_empresa) STRING  
Na tabela `rj-smtr.projeto_subsidio_sppo.viagem_completa`, temos a coluna id_empresa esta no formato STRING. Para otimizar o banco de dados, seria interessante transformar essa coluna em no tipo `INT` em vez de `STRING`. Isso reduziria a carga no banco e tornaria as consultas mais rápidas, STRING consomem mais bytes que um `SMALLINT`
**Impactos**:
- Consultas mais lentas devido ao formato atual.
- Armazenamento desperdiçado com dados no formato inadequado.

---

## 2. Inconsistência nas Estruturas das Tabelas `viagem_completa` e `viagem_planejada` 🔄
Ao comparar as tabelas `viagem_completa` e `viagem_planejada`, observa-se que as colunas estão fora de ordem, o que dificulta a análise, a manutenção e os futuros debugs. Além disso, a tabela `viagem_completa` possui colunas adicionais que não estão presentes na tabela `viagem_planejada (A do BigQuery não que realizei no projeto)`.

---

## 3. Inconsistência de Idioma nas Colunas 📝
Embora o padrão GTFS tenha sido seguido, ao analisar as tabelas `viagem_completa` e `viagem_planejada`, nota-se que algumas colunas estão em português, enquanto outras estão em inglês. Isso quebra a consistência do banco de dados, pois a maior parte das tabelas já utiliza inglês. Manter um único idioma em todas as colunas é crucial para evitar confusão e facilitar a análise e manutenção do banco de dados.

**Impactos**:
- Dificulta a análise e manutenção do banco de dados.
- Prejudica futuros debugs.

---

## 4. Problema na Coluna `versao_modelo` da Tabela `viagem_completa` 🛠️
A tabela `viagem_completa` possui a coluna `versao_modelo`, que está no formato STRING e contém espaços em branco no final do dado. Isso prejudica a análise e força o uso de funções como `TRIM`, o que gera carga desnecessária nas consultas. O ideal seria corrigir essa coluna na origem, aplicando um `UPDATE` na query e por fim ajustando o serviço que está povoando a tabela com dados incorretos.

**Impactos**:
- Consultas mais lentas devido à necessidade de manipulação dos dados.
- Armazenamento desperdiçado, pois espaços em branco também consomem recursos.

---

## 5. Dados Faltando na Tabela `viagem_completa` 🔍
Na tabela `viagem_completa`, a coluna `versao_modelo` contém valores nulos, o que afeta negativamente a junção com a tabela `feed_info`, resultando na perda de aproximadamente 45% dos dados. Isso compromete a integridade dos dados e a precisão das análises realizadas.

---

# REFERÊNCIAS 📚

- **GTFS**: [Documentação GTFS](https://gtfs.org/documentation/schedule)
- **Tipos de Dados do BigQuery**: [Referência BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)
- **Funções de Intervalo do BigQuery**: [Funções BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/interval_functions)
- **Funções Gerais do BigQuery**: [Funções BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-all)

**PROJETO rj_smtr**: [GitHub - Viagens 2 Ajustes](https://github.com/prefeitura-rio/pipelines_rj_smtr/tree/staging/viagens-2-ajustes)
