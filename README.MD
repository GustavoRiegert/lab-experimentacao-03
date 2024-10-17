# Análise de Pull Requests no GitHub

Este repositório contém uma análise das características dos Pull Requests (PRs) e como essas variáveis influenciam o feedback final e o número de revisões. Este projeto foi desenvolvido para o **6º período de Engenharia de Software**, como parte de uma disciplina de **Laboratório de Experimentação**.

## 📋 Visão Geral

O _code review_ é uma prática essencial para manter a qualidade de código nos projetos de software. Neste projeto, analisamos PRs de 200 dos repositórios mais populares do GitHub, explorando como o tamanho do PR, tempo de análise, descrição e interações afetam sua aprovação e revisão.

---

## 🛠️ Metodologia

### Seleção dos Repositórios
- 200 repositórios mais populares do GitHub.
- Repositórios com **pelo menos 100 PRs** (_merged_ + _closed_).

### Filtragem dos PRs
- Status de **MERGED** ou **CLOSED**.
- PRs com **pelo menos uma revisão**.
- Revisões com duração mínima de **uma hora**, para excluir análises automáticas.

### Coleta de Dados
- 25 PRs por repositório, com coleta limitada a **20 minutos** por repositório devido à API do GitHub.
- Coleta realizada ao longo de **12 horas**.

### Métricas Analisadas
- **Tamanho:** Arquivos modificados, linhas adicionadas e removidas.
- **Tempo de Análise:** Intervalo entre criação e fechamento/merge.
- **Descrição:** Número de caracteres do corpo em markdown.
- **Interações:** Número de participantes e comentários.

---

## 🔍 Questões de Pesquisa

### Feedback Final das Revisões
1. Relação entre o **tamanho dos PRs** e o feedback final.
2. Relação entre o **tempo de análise** e o feedback final.
3. Impacto da **descrição** na aprovação dos PRs.
4. Como as **interações** afetam o feedback final.

### Número de Revisões
5. Como o **tamanho** do PR impacta o número de revisões.
6. Relação entre o **tempo de análise** e o número de revisões.
7. Impacto da **descrição** no número de revisões.
8. Como as **interações** influenciam o número de revisões.

---

## 📊 Resultados

### Principais Conclusões
1. **Tamanho e Feedback:** PRs maiores tendem a receber mais feedback e revisões.
2. **Tempo e Feedback:** PRs analisados por mais tempo têm maior chance de rejeição.
3. **Descrição e Aprovação:** Descrições detalhadas aumentam a probabilidade de aprovação.
4. **Interações e Feedback Positivo:** Mais interações correlacionam-se com feedback positivo.

### Análise Estatística
- **Correlação de Pearson:** Identificou relação significativa entre o tamanho dos PRs e o feedback.
- **Regressão Linear:** Variáveis como tempo de análise e interações são preditores de feedback.
