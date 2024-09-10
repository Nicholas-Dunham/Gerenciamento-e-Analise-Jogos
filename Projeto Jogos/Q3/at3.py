import pandas as pd
from sqlalchemy import create_engine

# Exceções personalizadas
class ErroLeituraArquivo(Exception):
    pass

class ErroProcessamentoDados(Exception):
    pass

class ErroExportacaoBancoDados(Exception):
    pass

def ler_excel(caminho_arquivo):
    """
    Função para ler um arquivo Excel.

    Args:
        caminho_arquivo (str): Caminho para o arquivo Excel a ser lido.

    Returns:
        pd.DataFrame: DataFrame contendo os dados lidos do arquivo Excel.

    Raises:
        ErroLeituraArquivo: Se houver um erro ao ler o arquivo Excel.
    """
    try:
        df = pd.read_excel(caminho_arquivo)
        return df
    except Exception as e:
        raise ErroLeituraArquivo(f"Erro ao ler o arquivo Excel: {e}")

def analisar_jogos(df):
    """
    Função para analisar os jogos preferidos dos usuários.

    Args:
        df (pd.DataFrame): DataFrame contendo os dados a serem analisados.

    Returns:
        tuple: Uma tupla contendo três conjuntos:
               - Conjunto de todos os jogos mencionados pelos usuários.
               - Conjunto de jogos mencionados por apenas um usuário.
               - Dicionário com os jogos mais comuns e suas contagens.

    Raises:
        ErroProcessamentoDados: Se houver um erro ao processar os dados.
    """
    try:
        todos_jogos = set()
        conjuntos_jogos_usuario = []

        for jogos in df['jogos_preferidos']:
            conjunto_jogos = set(jogos.split('|'))
            conjuntos_jogos_usuario.append(conjunto_jogos)
            todos_jogos.update(conjunto_jogos)

        jogos_unicos = set()
        for jogo in todos_jogos:
            contagem = sum([jogo in conjunto for conjunto in conjuntos_jogos_usuario])
            if contagem == 1:
                jogos_unicos.add(jogo)

        contagem_jogos = {}
        for conjunto in conjuntos_jogos_usuario:
            for jogo in conjunto:
                if jogo in contagem_jogos:
                    contagem_jogos[jogo] += 1
                else:
                    contagem_jogos[jogo] = 1

        jogos_comuns = {jogo: contagem for jogo, contagem in contagem_jogos.items() if contagem > 1}

        return todos_jogos, jogos_unicos, jogos_comuns
    except Exception as e:
        raise ErroProcessamentoDados(f"Erro ao processar os dados: {e}")

def exportar_para_sqlite(todos_jogos, jogos_unicos, jogos_comuns, caminho_banco_dados):
    """
    Função para exportar dados para um banco de dados SQLite.

    Args:
        todos_jogos (set): Conjunto de todos os jogos mencionados.
        jogos_unicos (set): Conjunto de jogos mencionados por apenas um usuário.
        jogos_comuns (dict): Dicionário com jogos mais comuns e suas contagens.
        caminho_banco_dados (str): Caminho para o banco de dados SQLite.

    Raises:
        ErroExportacaoBancoDados: Se houver um erro ao exportar os dados para o banco de dados SQLite.
    """
    try:
        engine = create_engine(f'sqlite:///{caminho_banco_dados}')
        
        df_todos_jogos = pd.DataFrame(sorted(todos_jogos), columns=['jogo'])
        df_jogos_unicos = pd.DataFrame(sorted(jogos_unicos), columns=['jogo'])
        df_jogos_comuns = pd.DataFrame(sorted(jogos_comuns.items(), key=lambda x: (-x[1], x[0])), columns=['jogo', 'contagem'])
        
        df_todos_jogos.to_sql('todos_jogos', con=engine, if_exists='replace', index=False)
        df_jogos_unicos.to_sql('jogos_unicos', con=engine, if_exists='replace', index=False)
        df_jogos_comuns.to_sql('jogos_comuns', con=engine, if_exists='replace', index=False)
        
        print(f"Dados exportados com sucesso para o banco de dados SQLite em {caminho_banco_dados}")
    except Exception as e:
        raise ErroExportacaoBancoDados(f"Erro ao exportar os dados para o banco de dados SQLite: {e}")

def main():
    caminho_excel = 'Q3/dados_consolidados.xlsx'
    caminho_banco_dados = 'Q4/analise_jogos.db'
    
    try:
        df = ler_excel(caminho_excel)
    except ErroLeituraArquivo as e:
        print(e)
        return

    if not df.empty:
        try:
            todos_jogos, jogos_unicos, jogos_comuns = analisar_jogos(df)
        except ErroProcessamentoDados as e:
            print(e)
            return
        
        try:
            exportar_para_sqlite(todos_jogos, jogos_unicos, jogos_comuns, caminho_banco_dados)
        except ErroExportacaoBancoDados as e:
            print(e)
    else:
        print("Não foi possível realizar a análise devido a problemas na leitura do arquivo Excel.")

if __name__ == "__main__":
    main()
