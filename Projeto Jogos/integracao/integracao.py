import json
import pandas as pd
import requests
from sqlalchemy import create_engine, Column, Integer, String, Date, Text, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import logging
import datetime
import time
import re

# Configuração do logger
logging.basicConfig(filename='integracao/Log_erros.log', level=logging.INFO, format='%(asctime)s - %(message)s')

Base = declarative_base()

# Definição da tabela de usuários
class Usuario(Base):
    __tablename__ = 'usuarios'
    id = Column(Integer, primary_key=True)
    nome_completo = Column(String)
    email = Column(String)
    data_nascimento = Column(Date)
    cidade = Column(String)
    estado = Column(String)
    consoles = Column(Text)
    jogos_preferidos = Column(Text)

# Configuração do banco de dados
BANCO_DADOS = 'sqlite:///integracao/usuarios.db'
engine = create_engine(BANCO_DADOS)
Session = sessionmaker(bind=engine)
session = Session()

# Criação das tabelas
Base.metadata.create_all(engine)

def validar_nome(nome):
    """Valida o nome completo do usuário."""
    if not nome.strip():
        raise ValidationError("Nome completo é obrigatório.")
    return nome

def validar_email(email):
    """Valida o email do usuário."""
    if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
        raise ValidationError("E-mail inválido.")
    return email

def validar_data_nascimento(data_nascimento):
    """Valida a data de nascimento do usuário."""
    try:
        datetime.datetime.strptime(data_nascimento, '%Y-%m-%d')
        return data_nascimento
    except ValueError:
        raise ValidationError("Data de nascimento inválida.")
    
def validar_cidade(cidade):
    """Valida a cidade do usuário."""
    if not cidade.strip():
        raise ValidationError("Cidade é obrigatória.")
    return cidade

def validar_estado(estado):
    """Valida o estado do usuário."""
    if not estado.strip():
        raise ValidationError("Estado é obrigatório.")
    return estado

# Exceções personalizadas
class ValidationError(Exception):
    pass

class RegistroErro(Exception):
    pass

class ImportError(Exception):
    pass

class UpdateError(Exception):
    pass

class ErroRecomendacao(Exception):
    pass

class ErroPesquisa(Exception):
    pass

def converter_datas(df, colunas_data):
    """Converte colunas de datas para o formato datetime."""
    for coluna in colunas_data:
        df[coluna] = pd.to_datetime(df[coluna], errors='coerce')  # Converte datas e deixa inválidas como NaT (Not a Time)
    return df

# Define a função para atualizar o mapeamento
def atualizar_mapeamento(usuario):
    """Atualiza o mapeamento de jogos para usuários."""
    try:
        jogos = usuario.jogos_preferidos.split('|')
        for jogo in jogos:
            if jogo not in jogos_para_usuarios:
                jogos_para_usuarios[jogo] = set()
            jogos_para_usuarios[jogo].add(usuario.id)
    except Exception as e:
        raise UpdateError(f"Erro ao atualizar o mapeamento: {e}")

# Funções de importação com conversão de datas
def importar_usuarios_json(caminho_json):
    """Importa usuários de um arquivo JSON."""
    try:
        with open(caminho_json, 'r') as arquivo:
            usuarios = json.load(arquivo)
        df = pd.DataFrame(usuarios)
        df = converter_datas(df, ['data_nascimento'])
        for _, row in df.iterrows():
            usuario = Usuario(
                nome_completo=row['nome_completo'],
                email=row['email'],
                data_nascimento=row['data_nascimento'].date() if pd.notna(row['data_nascimento']) else None,
                cidade=row['cidade'],
                estado=row['estado'],
                consoles=row['consoles'],
                jogos_preferidos=row['jogos_preferidos']
            )
            session.add(usuario)
            session.commit()
            atualizar_mapeamento(usuario)
        print("Usuários importados com sucesso.")
    except Exception as e:
        session.rollback()
        raise ImportError(f"Erro ao importar usuários: {e}")

def importar_usuarios_csv(caminho_csv):
    """Importa usuários de um arquivo CSV."""
    try:
        df = pd.read_csv(caminho_csv)
        df = converter_datas(df, ['data_nascimento'])
        for _, row in df.iterrows():
            usuario = Usuario(
                nome_completo=row['nome_completo'],
                email=row['email'],
                data_nascimento=row['data_nascimento'].date() if pd.notna(row['data_nascimento']) else None,
                cidade=row['cidade'],
                estado=row['estado'],
                consoles=row['consoles'],
                jogos_preferidos=row['jogos_preferidos']
            )
            session.add(usuario)
            session.commit()
            atualizar_mapeamento(usuario)
        print("Usuários importados com sucesso do CSV.")
    except Exception as e:
        session.rollback()
        raise ImportError(f"Erro ao importar usuários: {e}")

def importar_usuarios_xlsx(caminho_xlsx):
    """Importa usuários de um arquivo XLSX."""
    try:
        df = pd.read_excel(caminho_xlsx)
        df = converter_datas(df, ['data_nascimento'])
        for _, row in df.iterrows():
            usuario = Usuario(
                nome_completo=row['nome_completo'],
                email=row['email'],
                data_nascimento=row['data_nascimento'].date() if pd.notna(row['data_nascimento']) else None,
                cidade=row['cidade'],
                estado=row['estado'],
                consoles=row['consoles'],
                jogos_preferidos=row['jogos_preferidos']
            )
            session.add(usuario)
            session.commit()
            atualizar_mapeamento(usuario)
        print("Usuários importados com sucesso do XLSX.")
    except Exception as e:
        session.rollback()
        raise ImportError(f"Erro ao importar usuários: {e}")

# Função de cadastro de usuário
def cadastrar_usuario():
    """Cadastra um novo usuário."""
    try:
        nome_completo = validar_nome(input("Nome: "))
        email = validar_email(input("Email: "))
        data_nascimento = validar_data_nascimento(input("Data de Nascimento (YYYY-MM-DD): "))
        cidade = validar_cidade(input("Cidade: "))
        estado = validar_estado(input("Estado: "))
        consoles = input("Consoles (separados por '|'): ")
        jogos_preferidos = input("Jogos Preferidos (separados por '|'): ")

        usuario = Usuario(
            nome_completo=nome_completo,
            email=email,
            data_nascimento=datetime.datetime.strptime(data_nascimento, '%Y-%m-%d').date(),
            cidade=cidade,
            estado=estado,
            consoles=consoles,
            jogos_preferidos=jogos_preferidos
        )
        session.add(usuario)
        session.commit()
        atualizar_mapeamento(usuario)
        print("Usuário cadastrado com sucesso.")
    except ValidationError as ve:
        print(f"Erro de validação: {ve}")
    except Exception as e:
        session.rollback()
        print(f"Erro ao cadastrar usuário: {e}")

# Função de recomendação de jogos
def recomendar_jogos():
    """Recomenda jogos para um usuário baseado nos jogos preferidos de outros usuários."""
    try:
        usuario_id = int(input("ID do usuário para o qual você deseja recomendações: "))
        usuario = session.query(Usuario).filter_by(id=usuario_id).first()
        if not usuario:
            print("Usuário não encontrado.")
            return

        # Lista de jogos do usuário
        jogos_usuario = set(usuario.jogos_preferidos.split('|'))
        
        # Recomendação de jogos
        recomendacoes = set()
        for jogo in jogos_para_usuarios:
            if jogo not in jogos_usuario:
                recomendacoes.add(jogo)

        recomendacoes = list(recomendacoes)[:5]  # Limitar a 5 recomendações

        if recomendacoes:
            print(f"Recomendações de jogos para {usuario.nome_completo}:")
            for recomendacao in recomendacoes:
                print(f"- {recomendacao}")
        else:
            print("Nenhuma recomendação de jogo encontrada.")
    except Exception as e:
        print(f"Erro ao recomendar jogos: {e}")

def alterar_cadastro():
    """Altera os dados de um usuário existente."""
    try:
        usuario_id = int(input("ID do usuário a ser alterado: "))
        usuario = session.query(Usuario).filter_by(id=usuario_id).first()
        if not usuario:
            print("Usuário não encontrado.")
            return
        
        usuario.nome_completo = validar_nome(input(f"Nome ({usuario.nome_completo}): "))
        usuario.email = validar_email(input(f"Email ({usuario.email}): ")) or usuario.email
        data_nascimento = validar_data_nascimento(input(f"Data de Nascimento (YYYY-MM-DD) ({usuario.data_nascimento}): "))
        usuario.data_nascimento = datetime.datetime.strptime(data_nascimento, '%Y-%m-%d').date()
        usuario.cidade = validar_cidade(input(f"Cidade ({usuario.cidade}): "))
        usuario.estado = validar_estado(input(f"Estado ({usuario.estado}): "))
        usuario.consoles = input(f"Consoles ({usuario.consoles}): ")
        usuario.jogos_preferidos = input(f"Jogos Preferidos ({usuario.jogos_preferidos}): ")

        session.commit()
        print("Usuário alterado com sucesso.")
    except ValueError as ve:
        print(f"Erro de valor: {ve}")
    except ValidationError as ve:
        print(f"Erro de validação: {ve}")
    except Exception as e:
        session.rollback()
        print(f"Erro ao alterar cadastro: {e}")

def excluir_cadastro():
    """Exclui o cadastro de um usuário."""
    try:
        usuario_id = int(input("ID do usuário a ser excluído: "))
        usuario = session.query(Usuario).filter_by(id=usuario_id).first()
        if not usuario:
            print("Usuário não encontrado.")
            return

        session.delete(usuario)
        session.commit()
        print("Usuário excluído com sucesso.")
    except ValueError as ve:
        print(f"Erro de valor: {ve}")
    except Exception as e:
        session.rollback()
        print(f"Erro ao excluir cadastro: {e}")

def visualizar_cadastros():
    """Exibe todos os cadastros de usuários."""
    try:
        usuarios = session.query(Usuario).all()
        for usuario in usuarios:
            print(f"ID: {usuario.id}, Nome: {usuario.nome_completo}, Email: {usuario.email}, Data de Nascimento: {usuario.data_nascimento}, Cidade: {usuario.cidade}, Estado: {usuario.estado}, Consoles: {usuario.consoles}, Jogos Preferidos: {usuario.jogos_preferidos}")
    except Exception as e:
        print(f"Erro ao visualizar cadastros: {e}")

def consolidar_dados_para_xlsx():
    """Consolida os dados dos usuários em um arquivo XLSX."""
    try:
        usuarios = session.query(Usuario).all()
        dados = [{
            "ID": usuario.id,
            "Nome": usuario.nome_completo,
            "Email": usuario.email,
            "Data de Nascimento": usuario.data_nascimento,
            "Cidade": usuario.cidade,
            "Estado": usuario.estado,
            "Consoles": usuario.consoles,
            "Jogos Preferidos": usuario.jogos_preferidos
        } for usuario in usuarios]

        df = pd.DataFrame(dados)
        df.to_excel('usuarios_consolidados.xlsx', index=False)
        print("Dados consolidados exportados para usuarios_consolidados.xlsx")
    except Exception as e:
        print(f"Erro ao consolidar dados: {e}")

def mostrar_precos_jogos_preferidos():
    """Mostra os preços dos jogos preferidos de um usuário usando a API do Mercado Livre."""
    try:
        usuario_id = int(input("ID do usuário: "))
        usuario = session.query(Usuario).filter_by(id=usuario_id).first()
        if not usuario:
            print("Usuário não encontrado.")
            return

        jogos = usuario.jogos_preferidos.split('|')
        for jogo in jogos:
            response = requests.get(f'https://api.mercadolibre.com/sites/MLB/search?q={jogo}')
            results = response.json()['results']
            if results:
                menor_preco = min(item['price'] for item in results)
                print(f"Menor preço para {jogo}: R${menor_preco}")
            else:
                print(f"Jogo {jogo} não encontrado no Mercado Livre.")
            time.sleep(1)  # Respeitar o limite de taxa da API
    except ValueError as ve:
        print(f"Erro de valor: {ve}")
    except Exception as e:
        print(f"Erro ao buscar preços: {e}")

def pesquisar_jogos():
    """Pesquisa preços de jogos no Mercado Livre e encontra o menor preço disponível."""
    try:
        nome_jogo = input("Nome do jogo: ")
        menor_preco = float('inf')
        link_menor_preco = ""
        consoles = ["Playstation 4", "Playstation 5", "PS4", "PS5", "Xbox 360", "Xbox Series S", "Xbox Series X", "Nintendo Switch"]
        lista_negra = ["Amiibo"]
        palavras_nome_jogo = nome_jogo.lower().split()
        url = f"https://api.mercadolibre.com/sites/MLB/search?category=MLB186456&q={nome_jogo}"
        resposta = requests.get(url)
        resposta.raise_for_status()  # Levanta um HTTPError para respostas ruins
        dados = resposta.json()
        resultados = dados.get('results', [])

        # Filtrar resultados
        resultados_validos = [
            {'nome': item['title'], 'preco': item['price'], 'permalink': item['permalink']}
            for item in resultados if 'permalink' in item and item['permalink']
            and (
                item['title'].lower().startswith('jogo')
                or any(console.lower() in item['title'].lower() for console in consoles)
            )
            and not any(termo.lower() in item['title'].lower() for termo in lista_negra)
            and all(palavra in item['title'].lower() for palavra in palavras_nome_jogo)  # Verifica se todas as palavras do jogo estão presentes no título
        ]
        # Itera pelos resultados para encontrar o menor preço e seu link
        for item in resultados_validos:
            preco = item['preco']
            if preco < menor_preco:
                menor_preco = preco
                link_menor_preco = item['permalink']
        print(f"Menor preço para {nome_jogo}: R${menor_preco}")
        print(f"Link: {link_menor_preco}")
    except Exception as e:
        print(f"Erro ao pesquisar jogos: {e}")

def menu_principal():
    """Exibe o menu principal do sistema."""
    while True:
        try:
            print("\nMenu Principal:")
            print("1. Gerenciar cadastros")
            print("2. Consolidar dados para XLSX")
            print("3. Mostrar preços de jogos preferidos")
            print("4. Pesquisar jogos")
            print("5. Recomendar jogos")
            print("6. Importar usuários JSON")
            print("7. Importar usuários CSV")
            print("8. Importar usuários XLSX")
            print("9. Sair")

            opcao = input("Escolha uma opção: ")

            if opcao == '1':
                menu_cadastros()
            elif opcao == '2':
                consolidar_dados_para_xlsx()
            elif opcao == '3':
                mostrar_precos_jogos_preferidos()
            elif opcao == '4':
                pesquisar_jogos()
            elif opcao == '5':
                recomendar_jogos()
            elif opcao == '6':
                caminho_json = input("Caminho do arquivo JSON: ")
                importar_usuarios_json(caminho_json)
            elif opcao == '7':
                caminho_csv = input("Caminho do arquivo CSV: ")
                importar_usuarios_csv(caminho_csv)
            elif opcao == '8':
                caminho_xlsx = input("Caminho do arquivo XLSX: ")
                importar_usuarios_xlsx(caminho_xlsx)
            elif opcao == '9':
                print("Saindo...")
                break
            else:
                print("Opção inválida. Tente novamente.")
        except Exception as e:
            print(f"Ocorreu um erro: {e}")

def menu_cadastros():
    """Exibe o menu de gerenciamento de cadastros de usuários."""
    while True:
        try:
            print("\nMenu de Cadastros:")
            print("1. Cadastrar usuário")
            print("2. Alterar cadastro")
            print("3. Excluir cadastro")
            print("4. Visualizar cadastros")
            print("5. Voltar")

            opcao = input("Escolha uma opção: ")

            if opcao == '1':
                cadastrar_usuario()
            elif opcao == '2':
                alterar_cadastro()
            elif opcao == '3':
                excluir_cadastro()
            elif opcao == '4':
                visualizar_cadastros()
            elif opcao == '5':
                break
            else:
                print("Opção inválida. Tente novamente.")
        except Exception as e:
            print(f"Ocorreu um erro: {e}")

jogos_para_usuarios = {}

# Executa o menu principal
if __name__ == "__main__":
    menu_principal()
