# Sistema de Gerenciamento e Análise de Dados de Jogos

## Descrição

Este projeto foi desenvolvido como parte de uma avaliação para a disciplina de **Programação com Python** na faculdade. O objetivo principal foi criar um sistema completo para **gerenciamento** e **análise de dados de jogos**, utilizando diferentes técnicas de manipulação de dados, como web scraping, operações com conjuntos (sets) e integração com APIs. O sistema é composto por quatro mini-projetos que se integram para formar uma solução robusta e funcional.

## Mini-Projetos

### 1. Web Scraping de Jogos da Wikipédia
**Funcionalidade**: Extrai tabelas de jogos de páginas da Wikipédia, realiza a limpeza dos dados e exporta para formatos CSV, JSON ou Excel.  
**Tecnologias Utilizadas**: `BeautifulSoup`, `pandas`.  
**Objetivos**:
- Web scraping de tabelas.
- Limpeza e exportação de dados.

### 2. Leitura, Limpeza e Consolidação de Arquivos de Usuários
**Funcionalidade**: Lê dados de usuários em arquivos CSV, JSON e Excel, realiza a limpeza e consolida em um único arquivo Excel.  
**Tecnologias Utilizadas**: `pandas`.  
**Objetivos**:
- Leitura e manipulação de dados de diferentes formatos.
- Consolidação e exportação de dados.

### 3. Operações com Sets
**Funcionalidade**: Realiza operações com conjuntos (sets) em jogos relatados, identificando jogos únicos e mais populares. Os dados são exportados para um banco de dados SQLite.  
**Tecnologias Utilizadas**: `pandas`, `SQLAlchemy`, `SQLite`.  
**Objetivos**:
- Análise de dados com conjuntos (sets).
- Armazenamento em banco de dados SQL.

### 4. Extração de Dados da API do Mercado Livre
**Funcionalidade**: Consome a API do Mercado Livre para obter dados de preço e informações detalhadas dos jogos, e armazena em um banco de dados SQL.  
**Tecnologias Utilizadas**: `requests`, `SQLAlchemy`, `pandas`.  
**Objetivos**:
- Consumo de APIs RESTful.
- Manipulação e armazenamento de dados em SQL.

## Funcionalidades Gerais
- Integração dos quatro mini-projetos.
- Exportação de dados em formatos CSV, JSON e Excel.
- Manipulação e armazenamento em banco de dados SQL.
- Análise de dados utilizando conjuntos (sets).

## Tecnologias Utilizadas
- `Python`
- `pandas`
- `BeautifulSoup`
- `SQLAlchemy`
- `SQLite`
- `requests`

## Requisitos

Certifique-se de ter as seguintes bibliotecas instaladas para executar o projeto:

pip install pandas beautifulsoup4 requests sqlalchemy sqlite3

# Como Executar

1. Clone o repositório:

git clone https://github.com/Nicholas-Dunham/Gerenciamento-e-Analise-Jogos.git

2. Acesse a pasta do projeto:

cd Gerenciamento-e-Analise-Jogos

3.Execute os scripts de cada mini-projeto individualmente e, então, integre-os conforme descrito.

## Licença
Este projeto está licenciado sob a MIT License.
