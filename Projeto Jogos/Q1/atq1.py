import requests
from bs4 import BeautifulSoup
import pandas as pd

# Funções de extração, limpeza e exportação com tratamento de exceções personalizado

class WebScrapingError(Exception):
    """Classe base para exceções de web scraping."""
    pass

class RequisicaoError(WebScrapingError):
    """Exceção para erros de requisição."""
    def __init__(self, url, mensagem="Erro na requisição"):
        self.url = url
        self.mensagem = f"{mensagem} para {url}"
        super().__init__(self.mensagem)

class ProcessamentoError(WebScrapingError):
    """Exceção para erros de processamento da página."""
    def __init__(self, url, mensagem="Erro ao processar a página"):
        self.url = url
        self.mensagem = f"{mensagem} {url}"
        super().__init__(self.mensagem)

class LimpezaError(WebScrapingError):
    """Exceção para erros na limpeza dos dados."""
    def __init__(self, mensagem="Erro ao limpar dados"):
        self.mensagem = mensagem
        super().__init__(self.mensagem)

class ExportacaoError(WebScrapingError):
    """Exceção para erros na exportação dos dados."""
    def __init__(self, mensagem="Erro ao exportar dados"):
        self.mensagem = mensagem
        super().__init__(self.mensagem)


def extrair_tabelas(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        tabelas = soup.find_all('table', {'class': 'wikitable'})
        
        lista_dfs = []
        for tabela in tabelas:
            linhas = tabela.find_all('tr')
            if len(linhas) > 9:
                df = pd.read_html(str(tabela))[0]
                lista_dfs.append(df)
        
        return lista_dfs
    except requests.exceptions.RequestException as e:
        raise RequisicaoError(url, str(e))
    except Exception as e:
        raise ProcessamentoError(url, str(e))

def limpar_dados(dfs):
    try:
        dfs_limpos = []
        for df in dfs:
            df = df.dropna(how='all')
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].str.strip()
            df = df.fillna('')
            dfs_limpos.append(df)
        return dfs_limpos
    except Exception as e:
        raise LimpezaError(str(e))

def exportar_dados(df, nome_arquivo):
    try:
        arquivo_csv = f'Q1/{nome_arquivo}.csv'
        arquivo_json = f'Q1/{nome_arquivo}.json'
        arquivo_excel = f'Q1/{nome_arquivo}.xlsx'
        
        df = df.reset_index(drop=True)
        
        df.to_csv(arquivo_csv, index=False)
        df.to_json(arquivo_json, orient='records', indent=4, force_ascii=False)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [' '.join(col).strip() for col in df.columns.values]
        df.to_excel(arquivo_excel, index=False)
        
        print(f"Dados exportados para {arquivo_csv}, {arquivo_json} e {arquivo_excel}")
    except Exception as e:
        raise ExportacaoError(str(e))

# URLs das páginas da Wikipédia
urls = [
    'https://pt.wikipedia.org/wiki/Lista_de_jogos_para_PlayStation_5',
    'https://pt.wikipedia.org/wiki/Lista_de_jogos_para_PlayStation_4',
    'https://pt.wikipedia.org/wiki/Lista_de_jogos_para_Xbox_Series_X_e_Series_S',
    'https://pt.wikipedia.org/wiki/Lista_de_jogos_para_Xbox_360',
    'https://pt.wikipedia.org/wiki/Lista_de_jogos_para_Nintendo_Switch'
]

def main():
    dfs_por_console = {}
    
    for url in urls:
        nome_pagina = url.split('/')[-1]
        console = nome_pagina.replace('Lista_de_jogos_para_', '')
        
        try:
            tabelas = extrair_tabelas(url)
            tabelas_limpas = limpar_dados(tabelas)
            
            if console not in dfs_por_console:
                dfs_por_console[console] = []
            
            dfs_por_console[console].extend(tabelas_limpas)
        except WebScrapingError as e:
            print(e)
    
    for console, dfs in dfs_por_console.items():
        df_concatenado = pd.concat(dfs, ignore_index=True)
        exportar_dados(df_concatenado, f'{console}_jogos')

# Script principal
if __name__ == "__main__":
    main()
