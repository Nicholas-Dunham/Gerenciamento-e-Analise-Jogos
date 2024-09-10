import pandas as pd
import json
import re
from datetime import datetime

# Definindo classes de exceção personalizadas
class ErroLeituraArquivo(Exception):
    def __init__(self, mensagem):
        super().__init__(f"Erro ao ler o arquivo: {mensagem}")

class ErroValidacaoDados(Exception):
    def __init__(self, mensagem):
        super().__init__(f"Erro na validação dos dados: {mensagem}")

class ErroExportacaoDados(Exception):
    def __init__(self, mensagem):
        super().__init__(f"Erro ao exportar os dados: {mensagem}")

# Função para ler arquivos CSV, Excel e JSON
def ler_arquivos():
    """
    Função para ler arquivos CSV, Excel e JSON.

    Returns:
        list: Lista contendo DataFrames lidos de cada tipo de arquivo.

    Raises:
        ErroLeituraArquivo: Se ocorrer um erro ao ler qualquer um dos arquivos.
    """
    dfs = []
    
    try:
        df_csv = pd.read_csv('Q2/dadosAT.csv', encoding='utf-8')
        dfs.append(df_csv)
    except UnicodeDecodeError:
        df_csv = pd.read_csv('Q2/dadosAT.csv', encoding='latin1')
        dfs.append(df_csv)
    except FileNotFoundError:
        print("Arquivo CSV não encontrado, continuando sem o CSV.")
    except Exception as e:
        raise ErroLeituraArquivo(f"CSV: {e}")

    try:
        df_excel = pd.read_excel('Q2/dadosATNovo.xlsx', engine='openpyxl')
        dfs.append(df_excel)
    except Exception as e:
        raise ErroLeituraArquivo(f"Excel: {e}")

    try:
        with open('Q2/dadosATNovo.json', 'r', encoding='utf-8') as file:
            data_json = json.load(file)
            df_json = pd.DataFrame(data_json)
            dfs.append(df_json)
    except FileNotFoundError:
        print("Arquivo JSON não encontrado, continuando sem o JSON.")
    except Exception as e:
        raise ErroLeituraArquivo(f"JSON: {e}")

    if not dfs:
        raise ErroLeituraArquivo("Nenhum arquivo foi carregado com sucesso.")
    
    return dfs

# Função para validar email
def eh_email_valido(email):
    """
    Função para validar um endereço de email.

    Args:
        email (str): Endereço de email a ser validado.

    Returns:
        bool: True se o email for válido, False caso contrário.
    """
    regex = r'^\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    return re.match(regex, email) is not None

# Função para validar data
def validar_data(data):
    """
    Função para validar e converter uma string de data em formato padrão.

    Args:
        data (str): String contendo a data a ser validada e convertida.

    Returns:
        str: Data no formato 'YYYY-MM-DD' se válida, 'Data inválida' caso contrário.
    """
    formatos = ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"]
    for fmt in formatos:
        try:
            return datetime.strptime(data, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return 'Data inválida'

# Função para limpar e consolidar dados
def limpar_e_consolidar_dados(df):
    """
    Função para limpar e consolidar dados de um DataFrame.

    Args:
        df (pd.DataFrame): DataFrame contendo os dados a serem limpos e consolidados.

    Returns:
        pd.DataFrame: DataFrame limpo e consolidado.

    Raises:
        ErroValidacaoDados: Se ocorrer um erro na limpeza e validação dos dados.
    """
    try:
        df['data_nascimento'] = df['data_nascimento'].apply(validar_data)
        df['email'] = df['email'].apply(lambda x: x if eh_email_valido(x) else 'email inválido')

        # Preencher valores ausentes
        df.fillna({'estado': 'Desconhecido', 'consoles': 'Nenhum', 'jogos_preferidos': 'Nenhum'}, inplace=True)
    except Exception as e:
        raise ErroValidacaoDados(f"Erro na limpeza e validação dos dados: {e}")
    
    return df

def consolidar_dados(dfs):
    """
    Função para consolidar múltiplos DataFrames em um único DataFrame.

    Args:
        dfs (list): Lista de DataFrames a serem consolidados.

    Returns:
        pd.DataFrame: DataFrame consolidado.

    Notes:
        A função agrupa os dados pelo campo 'nome_completo' e aplica regras para consolidar linhas.

    """
    df_combinado = pd.concat(dfs, ignore_index=True)

    # Remover duplicatas baseadas nas regras fornecidas
    def consolidar_linhas(grupo_dados):
        email_valido = next((email for email in grupo_dados['email'] if eh_email_valido(email)), 'email inválido')
        data_valida = next((data for data in grupo_dados['data_nascimento'] if data != 'Data inválida'), 'Data inválida')
        return pd.Series({
            'nome_completo': grupo_dados['nome_completo'].iloc[0],
            'data_nascimento': data_valida,
            'email': email_valido,
            'cidade': grupo_dados['cidade'].iloc[0],
            'estado': grupo_dados['estado'].iloc[0],
            'consoles': grupo_dados['consoles'].iloc[0],
            'jogos_preferidos': grupo_dados['jogos_preferidos'].iloc[0]
        })

    df_combinado = df_combinado.groupby('nome_completo').apply(consolidar_linhas).reset_index(drop=True)

    return df_combinado

def exportar_para_excel(df, nome_arquivo='dados_consolidados.xlsx'):
    """
    Função para exportar um DataFrame para um arquivo Excel.

    Args:
        df (pd.DataFrame): DataFrame a ser exportado.
        nome_arquivo (str, optional): Nome do arquivo de saída. Defaults to 'dados_consolidados.xlsx'.

    Raises:
        ErroExportacaoDados: Se ocorrer um erro ao exportar os dados para o arquivo Excel.
    """
    try:
        df.to_excel(nome_arquivo, index=False)
        print(f"Dados exportados com sucesso para {nome_arquivo}")
    except Exception as e:
        raise ErroExportacaoDados(f"Excel: {e}")

def main():
    try:
        dfs = ler_arquivos()
        dfs_limpos = [limpar_e_consolidar_dados(df) for df in dfs]
        df_consolidado = consolidar_dados(dfs_limpos)
        exportar_para_excel(df_consolidado)
    except (ErroLeituraArquivo, ErroValidacaoDados, ErroExportacaoDados) as e:
        print(e)

if __name__ == "__main__":
    main()
