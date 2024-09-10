import pandas as pd
import requests
import sqlite3
import time
from sqlalchemy import create_engine
import logging
import re

# Configuração do logger
logging.basicConfig(filename='Q4/links_invalidos.log', level=logging.INFO,
                    format='%(asctime)s - %(message)s')

# Exceções personalizadas
class ErroLeituraArquivo(Exception):
    pass

class ErroRequisicaoAPI(Exception):
    pass

class ErroProcessamentoDados(Exception):
    pass

class ErroExportacaoBanco(Exception):
    pass

# Função para listar tabelas no banco de dados SQLite
def listar_tabelas(caminho_db):
    """
    Função para listar as tabelas presentes em um banco de dados SQLite.

    Args:
        caminho_db (str): Caminho para o arquivo do banco de dados SQLite.

    Returns:
        pandas.DataFrame: DataFrame contendo os nomes das tabelas.

    Raises:
        ErroLeituraArquivo: Se ocorrer um erro ao listar as tabelas.
    """
    try:
        conn = sqlite3.connect(caminho_db)
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        tabelas = pd.read_sql(query, conn)
        conn.close()
        return tabelas
    except Exception as e:
        raise ErroLeituraArquivo(f"Erro ao listar tabelas do banco de dados SQLite: {e}")

# Função para ler uma tabela do banco de dados SQLite
def ler_tabela_sqlite(caminho_db, nome_tabela):
    """
    Função para ler uma tabela específica de um banco de dados SQLite.

    Args:
        caminho_db (str): Caminho para o arquivo do banco de dados SQLite.
        nome_tabela (str): Nome da tabela a ser lida.

    Returns:
        pandas.DataFrame: DataFrame contendo os dados da tabela.

    Raises:
        ErroLeituraArquivo: Se ocorrer um erro ao ler a tabela.
    """
    try:
        conn = sqlite3.connect(caminho_db)
        query = f"SELECT * FROM {nome_tabela}"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        raise ErroLeituraArquivo(f"Erro ao ler a tabela {nome_tabela} do banco de dados SQLite: {e}")


# Função para consultar a API do Mercado Livre com filtragem
def consultar_informacoes_jogo(nome_jogo):
    """
    Função para consultar a API do Mercado Livre e filtrar informações sobre um jogo.

    Args:
        nome_jogo (str): Nome do jogo a ser consultado.

    Returns:
        list: Lista de dicionários contendo informações válidas de jogos.

    Raises:
        ErroRequisicaoAPI: Se ocorrer um erro na requisição à API do Mercado Livre.
    """
    try:
        consoles = ["Playstation 4", "Playstation 5", "PS4", "PS5", "Xbox 360", "Xbox Series S", "Xbox Series X", "Nintendo Switch"]
        lista_negra = ["Amiibo"]
        palavras_nome_jogo = nome_jogo.lower().split()
        url = f"https://api.mercadolibre.com/sites/MLB/search?category=MLB186456&q={nome_jogo}"
        resposta = requests.get(url)
        resposta.raise_for_status()  # Levanta um HTTPError para respostas ruins
        dados = resposta.json()
        resultados = dados.get('results', [])

        resultados_validos = [
            {'nome': item['title'], 'preco': item['price'], 'permalink': item['permalink']}
            for item in resultados if 'permalink' in item and item['permalink']
            and (
                item['title'].lower().startswith('jogo')
                or any(console.lower() in item['title'].lower() for console in consoles)
            )
            and not any(termo.lower() in item['title'].lower() for termo in lista_negra)
            and all(palavra in item['title'].lower() for palavra in palavras_nome_jogo) 
        ]

        if not resultados_validos:
            logging.info(f"Jogo sem permalink válido encontrado: {nome_jogo}")

        return resultados_validos
    except requests.exceptions.RequestException as e:
        raise ErroRequisicaoAPI(f"Erro ao consultar a API do Mercado Livre para o jogo {nome_jogo}: {e}")

def exportar_para_sqlite(dados, caminho_db):
    """
    Função para exportar os preços para um banco de dados SQLite.

    Args:
        dados (list): Lista de dicionários contendo os dados a serem exportados.
        caminho_db (str): Caminho para o arquivo do banco de dados SQLite.

    Raises:
        ErroExportacaoBanco: Se ocorrer um erro ao exportar os dados para o banco de dados SQLite.
    """
    try:
        engine = create_engine(f'sqlite:///{caminho_db}')
        df = pd.DataFrame(dados)
        df.to_sql('precos_jogos', con=engine, if_exists='replace', index=False)
        print(f"Dados exportados com sucesso para o banco de dados SQLite em {caminho_db}")
    except Exception as e:
        raise ErroExportacaoBanco(f"Erro ao exportar os dados para o banco de dados SQLite: {e}")

# Função principal para ler, consultar a API e exportar os dados
def principal(caminho_db_consolidado, caminho_db_saida):
    """
    Função principal que coordena a leitura de dados, consulta à API e exportação para um banco de dados.

    Args:
        caminho_db_consolidado (str): Caminho para o arquivo do banco de dados SQLite de entrada.
        caminho_db_saida (str): Caminho para o arquivo do banco de dados SQLite de saída.
    """
    try:
        tabelas = listar_tabelas(caminho_db_consolidado)
        print("Tabelas no banco de dados:", tabelas)
    except ErroLeituraArquivo as e:
        print(e)
        return
    
    nomes_tabelas = ['todos_jogos']
    todos_jogos = set()

    for nome_tabela in nomes_tabelas:
        if nome_tabela in tabelas['name'].values:
            try:
                df = ler_tabela_sqlite(caminho_db_consolidado, nome_tabela)
            except ErroLeituraArquivo as e:
                print(e)
                continue

            if not df.empty:
                todos_jogos.update(df['jogo'].tolist())
            else:
                print(f"A tabela {nome_tabela} está vazia ou não pôde ser lida.")
        else:
            print(f"A tabela {nome_tabela} não foi encontrada no banco de dados.")

    informacoes_jogos = []
    for jogo in todos_jogos:
        try:
            info_jogo = consultar_informacoes_jogo(jogo)
        except ErroRequisicaoAPI as e:
            print(e)
            continue
        
        informacoes_jogos.extend(info_jogo)
        time.sleep(1)  # Adiciona um atraso para respeitar os limites de taxa da API

    if informacoes_jogos:
        try:
            exportar_para_sqlite(informacoes_jogos, caminho_db_saida)
        except ErroExportacaoBanco as e:
            print(e)
    else:
        print("Nenhuma informação de jogo foi obtida da API.")

# Caminhos para o banco de dados de entrada e saída
caminho_db_consolidado = 'Q4/analise_jogos.db'
caminho_db_saida = 'Q4/mercado_livre_jogos.db'

# Executa o script principal
if __name__ == "__main__":
    principal(caminho_db_consolidado, caminho_db_saida)
