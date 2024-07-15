from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_socketio import SocketIO, emit
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import sqlite3
import requests
import os
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")

def get_db_connection(database_name):
    conn = sqlite3.connect(database_name)
    conn.row_factory = sqlite3.Row
    return conn

def create_tables(database_name):
    conn = get_db_connection(database_name)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ordens_servico (
            so TEXT PRIMARY KEY,
            numero_protocolo TEXT,
            id_planos_usuarios INTEGER,
            contrato INTEGER,
            tecnico TEXT,
            empresa TEXT,
            causa TEXT,
            cidade TEXT,
            descricao_regiao TEXT,
            status_os TEXT,
            cto_latitude REAL,
            cto_longitude REAL,
            data_agendamento DATE,
            data_abertura_os DATE,
            hora_abertura_os TEXT,
            data_finalizacao_os DATE,
            adminversion INTEGER,
            final_login TEXT,
            final_login_2 TEXT,
            Regional TEXT,
            descricao_os TEXT,
            dslam_projeto TEXT,
            hostname TEXT,
            primaria TEXT,
            caixa TEXT,
            analisado BOOLEAN DEFAULT 0,
            comentario TEXT,
            usuario TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS comentarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ordem_servico TEXT,
            comentario TEXT,
            usuario TEXT,
            data_comentario DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (ordem_servico) REFERENCES ordens_servico(so)
        )
    ''')

    conn.commit()
    conn.close()

def inicializar_database():
    # Obter a data e hora atual no horário de Brasília
    now = datetime.now() - timedelta(hours=0)
    today = now.strftime('%Y-%m-%d')
    
    # Print da data e hora atual para verificação
    print(f"Data e hora atual (horário de Brasília): {now}")

    database_dir = 'public/bancodedados'
    if not os.path.exists(database_dir):
        os.makedirs(database_dir)
    database_name = os.path.join(database_dir, f'database{today}.db')
    if not os.path.exists(database_name):
        create_tables(database_name)
    return database_name

def criar_sessao_netcore():
    USER = 'maxwell.silva'
    PASSWORD = 'desktop@230270'
    login_url = "http://netcore.desktop.localdomain/usuarios/login"
    session = requests.Session()
    response = session.get(login_url)
    csrf_token = response.cookies.get('csrftoken', '')  # Garante um fallback para csrftoken

    payload_login = {
        "csrfmiddlewaretoken": csrf_token,
        "username": USER,
        "password": PASSWORD,
        "next": ""
    }
    
    result = session.post(login_url, data=payload_login, headers={"Referer": login_url})
    if result.status_code == 200:
        print("Login realizado com sucesso!")
    else:
        print(f"Falha no login com status {result.status_code}")
        raise Exception("Falha na autenticação")

    return session

def acessar_cliente(session, login):
    if not login or login.strip() == "":
        print(f"Login vazio. Pulando para o próximo.")
        return {"onu_rx": "N/A", "olt_rx": "N/A"}

    try:
        response_status = session.get(f"http://netcore.desktop.localdomain/weboss/get_status/{login}")
        if response_status.status_code == 200:
            dados_status = response_status.json().get('result', {})
            return {
                "onu_rx": dados_status.get("onu_rx", "N/A"),
                "olt_rx": dados_status.get("olt_rx", "N/A")
            }
        else:
            return {"onu_rx": "N/A", "olt_rx": "N/A"}
    except Exception as e:
        print(f"Erro ao acessar cliente: {str(e)}")
        return {"onu_rx": "N/A", "olt_rx": "N/A"}

def executar_consulta_e_salvar_db(database_name):
    conn_trino = connect(
        user="maxwell.silva",
        auth=BasicAuthentication("maxwell.silva", "desktop@230270"),
        http_scheme="https",
        port=443,
        host="k8s-srv01.intradesk",
        catalog="dw_desktop",
        schema="public",
        verify=False,
    )
    
    cur_trino = conn_trino.cursor()
    	   
    sql_query = """
WITH base AS (
    SELECT 
        os.so,
        os.numero_protocolo,
        p.id_cliente_contrato,
        os.tecnico,
        os.empresa,
        os.descricao,
        os.status_os,
        os.descricao_regiao,
        os.cidade,
        pu.id_cto,
        pu.cto_latitude,
        pu.cto_longitude,
        os.data_agendamento,
        CAST(os.data_abertura_os AS DATE) AS data_abertura_os,
        SPLIT_PART(CAST(os.data_abertura_os AS VARCHAR), ' ', 2) AS hora_abertura_os,
        os.data_finalizacao_os,
        p."ADMINVERSION",
        pu.login AS login
    FROM dw_desktop.public.ordem_servico os 
    LEFT JOIN dw_desktop.public.protocolos p ON p.numero_protocolo = os.numero_protocolo
    LEFT JOIN dw_desktop.public.planos_usuarios pu ON pu.id_planos_usuarios = p.id_cliente_contrato AND pu.recente = 1 
    WHERE p.adminversion = 3
      AND CAST(os.data_abertura_os AS DATE) = CURRENT_DATE
    UNION ALL
    SELECT 
        os.so,
        os.numero_protocolo,
        p.id_cliente_contrato,
        os.tecnico,
        os.empresa,
        os.descricao,
        os.status_os,
        os.descricao_regiao,
        os.cidade,
        pp.cto_id AS id_cto,
        pp.latitude_cto AS cto_latitude,
        pp.longitude_cto AS cto_longitude,
        os.data_agendamento,
        CAST(os.data_abertura_os AS DATE) AS data_abertura_os,
        SPLIT_PART(CAST(os.data_abertura_os AS VARCHAR), ' ', 2) AS hora_abertura_os,
        os.data_finalizacao_os,
        p.adminversion,
        NULL AS login  -- Este campo será preenchido na consulta final
    FROM dw_desktop.public.ordem_servico os 
    LEFT JOIN dw_desktop.public.protocolos p ON p.numero_protocolo = os.numero_protocolo
    LEFT JOIN dw_desktop.public.prospeccao_plano pp ON pp.id_prospeccao_plano = p.id_cliente_contrato
    WHERE p.adminversion = 1
        AND CAST(os.data_abertura_os AS DATE) = CURRENT_DATE
),
logins_adm1 AS (
    SELECT
        id_prospeccao_plano,
        CAST(id_adm AS VARCHAR) || '.1@desktop.com.br' AS login_adm
    FROM dw_desktop.public.pessoa_prospec_contrato_plano
),
procura_adm AS (
    SELECT
        b.*,
        COALESCE(la.login_adm, b.login) AS final_login
    FROM base b
    LEFT JOIN logins_adm1 la ON b.id_cliente_contrato = la.id_prospeccao_plano AND b.adminversion = 1
),
planos_usuarios_info AS (
    SELECT
        id_planos_usuarios,
        CAST(id_adm AS VARCHAR) || '.1@desktop.com.br' AS final_login_2
    FROM dw_desktop.public.planos_usuarios
    WHERE recente = 1
),
filtered_planos_usuarios AS (
    SELECT
        pu.login,
        pu.cto_latitude,
        pu.cto_longitude,
        pu.dslam_projeto,
        pu.hostname,
        pu.primaria,
        pu.caixa,
        pu.status_plano_usuario,
        pu.ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY pu.login ORDER BY (CASE WHEN pu.status_plano_usuario = 'ATIVO' THEN 1 ELSE 2 END), pu.ultima_atualizacao DESC) AS rn
    FROM dw_desktop.public.planos_usuarios pu
    WHERE pu.recente = 1
),
filtered_planos_usuarios_recente0 AS (
    SELECT
        pu.login,
        pu.cto_latitude,
        pu.cto_longitude,
        pu.dslam_projeto,
        pu.hostname,
        pu.primaria,
        pu.caixa,
        pu.ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY pu.login ORDER BY pu.ultima_atualizacao DESC) AS rn
    FROM dw_desktop.public.planos_usuarios pu
    WHERE pu.recente = 0
),
final_planos_usuarios AS (
    SELECT
        fpu.login,
        fpu.cto_latitude,
        fpu.cto_longitude,
        COALESCE(fpu.dslam_projeto, fpu0.dslam_projeto) AS dslam_projeto,
        COALESCE(fpu.hostname, fpu0.hostname) AS hostname,
        COALESCE(fpu.primaria, fpu0.primaria) AS primaria,
        COALESCE(fpu.caixa, fpu0.caixa) AS caixa,
        fpu.status_plano_usuario,
        fpu.ultima_atualizacao
    FROM filtered_planos_usuarios fpu
    LEFT JOIN filtered_planos_usuarios_recente0 fpu0 ON fpu.login = fpu0.login AND fpu0.rn = 1
    WHERE fpu.rn = 1
)
SELECT
    pa.so,
    pa.numero_protocolo,
    CASE WHEN pa.adminversion = 3 THEN pa.id_cliente_contrato ELSE NULL END AS id_planos_usuarios,
    CASE WHEN pa.adminversion = 1 THEN pa.id_cliente_contrato ELSE NULL END AS contrato,
    pa.tecnico,
    pa.empresa,
    p.primeiro_procedimento AS causa, -- Adicionar esta linha
    pa.cidade,
    pa.descricao_regiao,
    pa.status_os,
    COALESCE(fpu.cto_latitude, pa.cto_latitude) AS cto_latitude,
    COALESCE(fpu.cto_longitude, pa.cto_longitude) AS cto_longitude,
    pa.data_agendamento,
    pa.data_abertura_os,
    pa.hora_abertura_os,
    pa.data_finalizacao_os,
    pa.adminversion,
    pa.final_login,
    CASE
        WHEN pa.adminversion = 3 THEN pu_info.final_login_2
        ELSE NULL
    END AS final_login_2,
    CASE
        WHEN cidade = 'Águas de Santa Bárbara' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Aguas de Santa Barbara' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Agudos' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Américo Brasiliense' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Americo Brasiliense' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Am. Brasiliense' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Arandu' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Araraquara' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Arealva' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Areiópolis' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Areiopolis' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Avaré' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Avare' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Bady Bassitt' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Bariri' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Barra Bonita' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Barretos' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Bauru' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Bebedouro' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Boa Esperança do Sul' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Boa Esperanca do Sul' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Bocaina' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Borborema' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Borebi' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Botucatu' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Cândido Rodrigues' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Candido Rodrigues' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Cerqueira César' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Cerqueira Cesar' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Colina' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Descalvado' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Dobrada' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Dois Córregos' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Dois Corregos' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Dourado' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Fernando Prestes' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Gavião Peixoto' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Gaviao Peixoto' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Guaíra' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Guaira' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Guariba' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Guatapará' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Guatapara' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Iacanga' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Iaras' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Ibaté' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Ibate' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Ibitinga' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Igaraçu do Tietê' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Igaracu do Tiete' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Igarapava' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Itajobi' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Itaí' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Itai' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Itaju' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Itápolis' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Itapolis' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Itapuí' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Itapui' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Itatinga' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Itirapina' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Jaborandi' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Jaboticabal' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Jaú' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Jau' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Lençóis Paulista' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Lencois Paulista' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Lins' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Macatuba' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Manduri' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Matão' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Matao' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Mineiros do Tietê' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Mineiros do Tiete' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Monte Alto' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Motuca' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Mirassol' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Nova Europa' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Novo Horizonte' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Óleo' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Oleo' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Olímpia' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Olimpia' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Paranapanema' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Pardinho' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Pederneiras' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Pindorama' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Piratininga' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Pitangueiras' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Pradópolis' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Pradopolis' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Pratânia' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Pratania' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Ribeirão Bonito' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Ribeirao Bonito' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Rincão' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Rincao' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'São Joaquim da Barra' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Sao Joaquim da Barra' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Santa Adélia' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Santa Adelia' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Santa Cruz do Rio Pardo' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Santa Ernestina' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Santa Lúcia' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Santa Lucia' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'São Carlos' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Sao Carlos' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'São José do Rio Preto' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Sao Jose do Rio Preto' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'S. J. Rio Preto' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'São Manuel' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Sao Manuel' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Tabatinga' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Trabiju' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Araraquara' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Avaré' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Avare' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Barretos' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Bauru' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Bebedouro' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Botucatu' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Guaíra' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Guaira' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Ibitinga' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Jaboticabal' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Jaú' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Jau' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Lençois Paulista' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Lencois Paulista' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Lins' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Olímpia' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Olimpia' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Pitangueiras' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'São Carlos' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Sao Carlos' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'São José do Rio Preto' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Sao Jose do Rio Preto' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Américo Brasiliense' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Americo Brasiliense' THEN 'Regional Centro - Oeste'
        WHEN cidade = 'Bertioga' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Biritiba Mirim' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Caçapava' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Cacapava' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Cubatão' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Cubatao' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Guararema' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Guarujá' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Guaruja' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Guaruja' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Igaratá' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Igarata' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Itanhaém' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Itanhaem' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Jacareí' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Jacarei' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Mogi das Cruzes' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Mongaguá' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Mongagua' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Peruíbe' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Peruibe' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Peruibe' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Praia Grande' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Salesópolis' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Salesopolis' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Santa Branca' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Santos' THEN 'Regional Vale & Sul'
        WHEN cidade = 'São Bernardo do Campo' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Sao Bernardo do Campo' THEN 'Regional Vale & Sul'
        WHEN cidade = 'S. B. do Campo' THEN 'Regional Vale & Sul'
        WHEN cidade = 'São José dos Campos' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Sao Jose dos Campos' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Alumínio' THEN 'Regional Central'
        WHEN cidade = 'Aluminio' THEN 'Regional Central'
        WHEN cidade = 'S. J. Campos' THEN 'Regional Vale & Sul'
        WHEN cidade = 'São Vicente' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Sao Vicente' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Taubaté' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Taubate' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Tremembé' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Tremembe' THEN 'Regional Vale & Sul'
        WHEN cidade = 'São Paulo' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Sao Paulo' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Guararema' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Praia Grande' THEN 'Regional Vale & Sul'
        WHEN cidade = 'São Bernardo do Campo' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Sao Bernardo do Campo' THEN 'Regional Vale & Sul'
        WHEN cidade = 'São José dos Campos' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Sao Jose dos Campos' THEN 'Regional Vale & Sul'
        WHEN cidade = 'São Vicente' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Sao Vicente' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Mogi das Cruzes' THEN 'Regional Vale & Sul'
        WHEN cidade = 'Sumaré' THEN 'Regional Central'
        WHEN cidade = 'Sumare' THEN 'Regional Central'
        WHEN cidade = 'Piracicaba' THEN 'Regional Central'
        WHEN cidade = 'Nova Odessa' THEN 'Regional Central'
        WHEN cidade = 'Santa Barbara do Oeste' THEN 'Regional Central'
        WHEN cidade = 'S. B. Oeste' THEN 'Regional Central'
        WHEN cidade = 'Santa Bárbara d''Oeste' THEN 'Regional Central'
        WHEN cidade = 'Americana' THEN 'Regional Central'
        WHEN cidade = 'Limeira' THEN 'Regional Central'
        WHEN cidade = 'Cordeirópolis' THEN 'Regional Central'
        WHEN cidade = 'Cordeiropolis' THEN 'Regional Central'
        WHEN cidade = 'Capivari' THEN 'Regional Central'
        WHEN cidade = 'Iracemápolis' THEN 'Regional Central'
        WHEN cidade = 'Iracemapolis' THEN 'Regional Central'
        WHEN cidade = 'Araras' THEN 'Regional Central'
        WHEN cidade = 'Santa Gertrudes' THEN 'Regional Central'
        WHEN cidade = 'Conchal' THEN 'Regional Central'
        WHEN cidade = 'Rafard' THEN 'Regional Central'
        WHEN cidade = 'Engenheiro Coelho' THEN 'Regional Central'
        WHEN cidade = 'Eng. Coelho' THEN 'Regional Central'
        WHEN cidade = 'Sorocaba' THEN 'Regional Central'
        WHEN cidade = 'Santa Rita do Passa Quatro' THEN 'Regional Central'
        WHEN cidade = 'S. R. P. Quatro' THEN 'Regional Central'
        WHEN cidade = 'Leme' THEN 'Regional Central'
        WHEN cidade = 'Estiva Gerbi' THEN 'Regional Central'
        WHEN cidade = 'Diadema' THEN 'Regional Central'
        WHEN cidade = 'Guará' THEN 'Regional Central'
        WHEN cidade = 'Guara' THEN 'Regional Central'
        WHEN cidade = 'Itu' THEN 'Regional Central'
        WHEN cidade = 'Aguaí' THEN 'Regional Central'
        WHEN cidade = 'Aguai' THEN 'Regional Central'
        WHEN cidade = 'São Simão' THEN 'Regional Central'
        WHEN cidade = 'Sao Simao' THEN 'Regional Central'
        WHEN cidade = 'Monte Mor' THEN 'Regional Central'
        WHEN cidade = 'São Roque' THEN 'Regional Central'
        WHEN cidade = 'Sao Roque' THEN 'Regional Central'
        WHEN cidade = 'Salto' THEN 'Regional Central'
        WHEN cidade = 'Angatuba' THEN 'Regional Central'
        WHEN cidade = 'Boituva' THEN 'Regional Central'
        WHEN cidade = 'Campina do Monte Alegre' THEN 'Regional Central'
        WHEN cidade = 'Capela do Alto' THEN 'Regional Central'
        WHEN cidade = 'Cerquilho' THEN 'Regional Central'
        WHEN cidade = 'Cesário Lange' THEN 'Regional Central'
        WHEN cidade = 'Cesario Lange' THEN 'Regional Central'
        WHEN cidade = 'Conchas' THEN 'Regional Central'
        WHEN cidade = 'Cristais Paulista' THEN 'Regional Central'
        WHEN cidade = 'Iperó' THEN 'Regional Central'
        WHEN cidade = 'Ipero' THEN 'Regional Central'
        WHEN cidade = 'Itirapuã' THEN 'Regional Central'
        WHEN cidade = 'Itirapua' THEN 'Regional Central'
        WHEN cidade = 'Jumirim' THEN 'Regional Central'
        WHEN cidade = 'Laranjal Paulista' THEN 'Regional Central'
        WHEN cidade = 'Patrocínio Paulista' THEN 'Regional Central'
        WHEN cidade = 'Patrocinio Paulista' THEN 'Regional Central'
        WHEN cidade = 'Pereiras' THEN 'Regional Central'
        WHEN cidade = 'Porangaba' THEN 'Regional Central'
        WHEN cidade = 'Quadra' THEN 'Regional Central'
        WHEN cidade = 'Ribeirão Corrente' THEN 'Regional Central'
        WHEN cidade = 'Ribeirao Corrente' THEN 'Regional Central'
        WHEN cidade = 'Rio das Pedras' THEN 'Regional Central'
        WHEN cidade = 'Saltinho' THEN 'Regional Central'
        WHEN cidade = 'Sarapuí' THEN 'Regional Central'
        WHEN cidade = 'Sarapui' THEN 'Regional Central'
        WHEN cidade = 'Tatuí' THEN 'Regional Central'
        WHEN cidade = 'Tatui' THEN 'Regional Central'
        WHEN cidade = 'Tietê' THEN 'Regional Central'
        WHEN cidade = 'Tiete' THEN 'Regional Central'
        WHEN cidade = 'Franca' THEN 'Regional Central'
        WHEN cidade = 'Casa Branca' THEN 'Regional Central'
        WHEN cidade = 'Mogi Mirim' THEN 'Regional Central'
        WHEN cidade = 'Pirassununga' THEN 'Regional Central'
        WHEN cidade = 'Porto Ferreira' THEN 'Regional Central'
        WHEN cidade = 'Rio Claro' THEN 'Regional Central'
        WHEN cidade = 'Mogi Guaçu' THEN 'Regional Central'
        WHEN cidade = 'Mogi Guacu' THEN 'Regional Central'
        WHEN cidade = 'Votorantim' THEN 'Regional Central'
        WHEN cidade = 'Santa Cruz das Palmeiras' THEN 'Regional Central'
        WHEN cidade = 'S. C. Palmeiras' THEN 'Regional Central'
        WHEN cidade = 'Araras' THEN 'Regional Central'
        WHEN cidade = 'Capivari' THEN 'Regional Central'
        WHEN cidade = 'Limeira' THEN 'Regional Central'
        WHEN cidade = 'Piracicaba' THEN 'Regional Central'
        WHEN cidade = 'Pirassununga' THEN 'Regional Central'
        WHEN cidade = 'Sorocaba' THEN 'Regional Central'
        WHEN cidade = 'Sumaré' THEN 'Regional Central'
        WHEN cidade = 'Sumare' THEN 'Regional Central'
        WHEN cidade = 'Franca' THEN 'Regional Central'
        WHEN cidade = 'Cerquilho' THEN 'Regional Central'
        WHEN cidade = 'Araçoiaba da Serra' THEN 'Regional Central'
        WHEN cidade = 'Aracoiaba da Serra' THEN 'Regional Central'
        WHEN cidade = 'Bofete' THEN 'Regional Central'
        WHEN cidade = 'Cosmópolis' THEN 'Regional Central'
        WHEN cidade = 'Cosmopolis' THEN 'Regional Central'
        WHEN cidade = 'Cosmopolis' THEN 'Regional Central'
        WHEN cidade = 'Cravinhos' THEN 'Regional Central'
        WHEN cidade = 'Paulínia' THEN 'Regional Central'
        WHEN cidade = 'Paulinia' THEN 'Regional Central'
        WHEN cidade = 'Pilar do Sul' THEN 'Regional Central'
        WHEN cidade = 'Ribeirão Preto' THEN 'Regional Central'
        WHEN cidade = 'Ribeirao Preto' THEN 'Regional Central'
        WHEN cidade = 'Campinas' THEN 'Regional Campinas'
        WHEN cidade = 'Jundiaí' THEN 'Regional Campinas'
        WHEN cidade = 'Jundiai' THEN 'Regional Campinas'
        WHEN cidade = 'Caieiras' THEN 'Regional Campinas'
        WHEN cidade = 'Várzea Paulista' THEN 'Regional Campinas'
        WHEN cidade = 'Varzea Paulista' THEN 'Regional Campinas'
        WHEN cidade = 'Franco da Rocha' THEN 'Regional Campinas'
        WHEN cidade = 'Indaiatuba' THEN 'Regional Campinas'
        WHEN cidade = 'Campo Limpo Paulista' THEN 'Regional Campinas'
        WHEN cidade = 'Campo Limpo Pt.' THEN 'Regional Campinas'
        WHEN cidade = 'Valinhos' THEN 'Regional Campinas'
        WHEN cidade = 'Vinhedo' THEN 'Regional Campinas'
        WHEN cidade = 'Itupeva' THEN 'Regional Campinas'
        WHEN cidade = 'Cabreúva' THEN 'Regional Campinas'
        WHEN cidade = 'Cabreuva' THEN 'Regional Campinas'
        WHEN cidade = 'Louveira' THEN 'Regional Campinas'
        WHEN cidade = 'Araçariguama' THEN 'Regional Campinas'
        WHEN cidade = 'Aracariguama' THEN 'Regional Campinas'
        WHEN cidade = 'Francisco Morato' THEN 'Regional Campinas'
        WHEN cidade = 'Francis. Morato' THEN 'Regional Campinas'
        WHEN cidade = 'Hortolândia' THEN 'Regional Campinas'
        WHEN cidade = 'Hortolandia' THEN 'Regional Campinas'
        WHEN cidade = 'Jaguariúna' THEN 'Regional Campinas'
        WHEN cidade = 'Jaguariuna' THEN 'Regional Campinas'
        WHEN cidade = 'Santo Antônio de Posse' THEN 'Regional Campinas'
        WHEN cidade = 'Santo Antonio de Posse' THEN 'Regional Campinas'
        WHEN cidade = 'St. Ant. Posse' THEN 'Regional Campinas'
        WHEN cidade = 'Holambra' THEN 'Regional Campinas'
        WHEN cidade = 'Pedreira' THEN 'Regional Campinas'
        WHEN cidade = 'Caieiras' THEN 'Regional Campinas'
        WHEN cidade = 'Sumare' THEN 'Regional Central'
        WHEN cidade = 'Mairipora' THEN 'Regional Campinas'
        WHEN cidade = 'Campinas' THEN 'Regional Campinas'
        WHEN cidade = 'Jaguariúna' THEN 'Regional Campinas'
        WHEN cidade = 'Jaguariuna' THEN 'Regional Campinas'
        WHEN cidade = 'Jundiaí' THEN 'Regional Campinas'
        WHEN cidade = 'Jundiai' THEN 'Regional Campinas'
        WHEN cidade = 'Amparo' THEN 'Regional Campinas'
        WHEN cidade = 'Atibaia' THEN 'Regional Campinas'
        WHEN cidade = 'Bom Jesus dos Perdões' THEN 'Regional Campinas'
        WHEN cidade = 'Bom Jesus dos Perdoes' THEN 'Regional Campinas'
        WHEN cidade = 'Bom Jesus dos P' THEN 'Regional Campinas'
        WHEN cidade = 'Bragança Paulista' THEN 'Regional Campinas'
        WHEN cidade = 'Braganca Paulista' THEN 'Regional Campinas'
        WHEN cidade = 'Itatiba' THEN 'Regional Campinas'
        WHEN cidade = 'Jarinu' THEN 'Regional Campinas'
        WHEN cidade = 'Lindóia' THEN 'Regional Campinas'
        WHEN cidade = 'Lindoia' THEN 'Regional Campinas'
        WHEN cidade = 'Mairiporã' THEN 'Regional Campinas'
        WHEN cidade = 'Mairipora' THEN 'Regional Campinas'
        WHEN cidade = 'Monte Alegre do Sul' THEN 'Regional Campinas'
        WHEN cidade = 'Nazaré Paulista' THEN 'Regional Campinas'
        WHEN cidade = 'Nazare Paulista' THEN 'Regional Campinas'
        WHEN cidade = 'Piracaia' THEN 'Regional Campinas'
        WHEN cidade = 'Serra Negra' THEN 'Regional Campinas'
    END AS Regional,
    CASE
        WHEN descricao  = 'Ativação GPON HOME' THEN 'Instalação'
        WHEN descricao  = 'Ativação EPMP' THEN 'Instalação'
        WHEN descricao  = 'Migração - Infolog' THEN 'Migração'
        WHEN descricao  = 'Ativação Rádio' THEN 'Instalação'
        WHEN descricao  = 'Ativacao TV 1 Ponto + Internet' THEN 'Instalação'
        WHEN descricao  = 'Ativação  VDSL' THEN 'Instalação'
        WHEN descricao  = 'Ativação GPON Dedicado / Corporativo' THEN 'Instalação'
        WHEN descricao  = 'Ativação  ADSL' THEN 'Instalação'
        WHEN descricao  = 'Ativacao VDSL + 1 ponto TV' THEN 'Instalação'
        WHEN descricao  = 'Mudança de Endereço TV GPON' THEN 'Mudança de Endereço'
        WHEN descricao  = 'Mudança de Endereço Fone Simples' THEN 'Mudança de Endereço'
        WHEN descricao  = 'Mudança de Endereço TV VDSL' THEN 'Mudança de Endereço'
        WHEN descricao  = 'Mudança de Endereço Rádio' THEN 'Mudança de Endereço'
        WHEN descricao  = 'Mudanca de Endereco GPON' THEN 'Mudança de Endereço'
        WHEN descricao  = 'Mudanca de Endereco ADSL e VDSL' THEN 'Mudança de Endereço'
        WHEN descricao  = 'Retirada de Equipamento - Compulsório ' THEN 'Retirada'
        WHEN descricao  = 'Retirada de Equipamento - Voluntário' THEN 'Retirada'
        WHEN descricao  = 'Retirada de Equipamento' THEN 'Retirada'
        WHEN descricao  = 'Manutenção/Suporte - Moto Desk' THEN 'Reparo'
        WHEN descricao  = 'Manutenção / Suporte' THEN 'Reparo'
        WHEN descricao  = 'Mudança de Ponto Interna GPON' THEN 'Mudança de Ponto'
        WHEN descricao  = 'Manutenção / Preventivas' THEN 'Preventiva'
        WHEN descricao  = 'Mudança de Ponto Externa GPON' THEN 'Reparo'
        WHEN descricao  = 'Suporte - Cliente conectado ou Com sinal' THEN 'Mudança de Ponto'
        WHEN descricao  = 'Mudança de Ponto Interna xDSL' THEN 'Mudança de Ponto'
        WHEN descricao  = 'Ativacao TV 2 Pontos' THEN 'Outros'
        WHEN descricao  = 'Ativacao TV 2 Pontos + Internet' THEN 'Outros'
        WHEN descricao  = 'Ativacao TV 4 Pontos + Internet' THEN 'Outros'
        WHEN descricao  = 'Ativacao TV 5 Pontos + Internet' THEN 'Outros'
        WHEN descricao  = 'Ativação Migração ADSL > GPON' THEN 'Migração'
        WHEN descricao  = 'Ativacao Migracao ADSL > VDSL' THEN 'Migração'
        WHEN descricao  = 'Ativacao TV 1 Ponto' THEN 'Outros'
        WHEN descricao  = 'Ativacao TV 3 Pontos' THEN 'Outros'
        WHEN descricao  = 'Troca de Plano GPON' THEN 'Upgrade'
        WHEN descricao  = 'Troca de ONU - Alteracao de Plano' THEN 'Upgrade'
        WHEN descricao  = 'Migração ZHONE' THEN 'Migração'
        WHEN descricao  = 'Reativação' THEN 'Instalação'
        WHEN descricao  = 'Ativação Migração GPON > ADSL' THEN 'Migração'
        WHEN descricao  = 'Ativacao Migracao Cond > ADSL' THEN 'Migração'
        WHEN descricao  = 'Ativacao Mudanca de Endereco + Instalacao TV VDSL' THEN 'Mudança de Endereço'
        WHEN descricao  = 'Ativacao TV 3 Pontos + Internet' THEN 'Outros'
        WHEN descricao  = 'Ativação Migração ADSL > Rádio' THEN 'Migração'
        WHEN descricao  = 'Mudança de Ponto Externa xDSL' THEN 'Outros'
        WHEN descricao  = 'Ativacao TV 4 Pontos' THEN 'Outros'
        WHEN descricao  = 'Ativacao TV 5 Pontos' THEN 'Outros'
        WHEN descricao  = 'Ativação Migração VDSL > Rádio' THEN 'Migração'
        WHEN descricao  = 'Ativacao Mudanca de Endereco + Instalacao TV GPON' THEN 'Mudança de Endereço'
        WHEN descricao  = 'Ativação Fone Simples' THEN 'Outros'
        WHEN descricao  = 'Ativacao ADSL e VDSL Condomonio' THEN 'Outros'
        WHEN descricao  = 'Ativacao VDSL + 2 pontos TV' THEN 'Outros'
        WHEN descricao  = 'Ativação Migração GPON > Rádio' THEN 'Migração'
        WHEN descricao  = 'Ativação Migração Rádio > ADSL' THEN 'Migração'
        WHEN descricao  = 'Ativacao Migracao Cond > GPON' THEN 'Migração'
        WHEN descricao  = 'Ativação Migração Rádio > GPON' THEN 'Migração'
        WHEN descricao  = 'Premium / GAED' THEN 'Premium'
        WHEN descricao  = 'COP - Correção de Ativação' THEN 'Reparo'
        WHEN descricao  = 'Regroup' THEN 'Regroup'
        WHEN descricao  = 'Migração de Base' THEN 'Migração'
    END AS descricao_os,
    fpu.dslam_projeto,
    fpu.hostname,
    fpu.primaria,
    fpu.caixa
FROM procura_adm pa
LEFT JOIN planos_usuarios_info pu_info ON pa.id_cliente_contrato = pu_info.id_planos_usuarios
LEFT JOIN final_planos_usuarios fpu ON pa.final_login = fpu.login
LEFT JOIN dw_desktop.public.protocolos p ON pa.numero_protocolo = p.numero_protocolo
WHERE pa.descricao IN ('Manutenção/Suporte - Moto Desk', 'Manutenção / Suporte', 'COP - Correção de Ativação')
AND pa.final_login IS NOT NULL
AND pa.status_os NOT IN ('Cancelado', 'Interrompido')

    """
    
    cur_trino.execute(sql_query)
    df_novos_dados = pd.DataFrame(cur_trino.fetchall(), columns=[desc[0] for desc in cur_trino.description])

    df_novos_dados['data_agendamento'] = df_novos_dados['data_agendamento'].astype(str)
    df_novos_dados['data_abertura_os'] = df_novos_dados['data_abertura_os'].astype(str)
    df_novos_dados['hora_abertura_os'] = df_novos_dados['hora_abertura_os'].astype(str)
    df_novos_dados['data_finalizacao_os'] = df_novos_dados['data_finalizacao_os'].astype(str)

    conn_sqlite = get_db_connection(database_name)
    cursor = conn_sqlite.cursor()

    for index, row in df_novos_dados.iterrows():
        cursor.execute('''
            INSERT INTO ordens_servico (
                so, numero_protocolo, id_planos_usuarios, contrato, tecnico, empresa, causa, cidade, descricao_regiao, status_os,
                cto_latitude, cto_longitude, data_agendamento, data_abertura_os, hora_abertura_os, data_finalizacao_os,
                adminversion, final_login, final_login_2, Regional, descricao_os, dslam_projeto, hostname, primaria, caixa,
                analisado, comentario, usuario
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(so) DO UPDATE SET
                numero_protocolo=excluded.numero_protocolo, id_planos_usuarios=excluded.id_planos_usuarios,
                contrato=excluded.contrato, tecnico=excluded.tecnico, empresa=excluded.empresa, causa=excluded.causa,
                cidade=excluded.cidade, descricao_regiao=excluded.descricao_regiao, status_os=excluded.status_os,
                cto_latitude=excluded.cto_latitude, cto_longitude=excluded.cto_longitude, data_agendamento=excluded.data_agendamento,
                data_abertura_os=excluded.data_abertura_os, hora_abertura_os=excluded.hora_abertura_os,
                data_finalizacao_os=excluded.data_finalizacao_os, adminversion=excluded.adminversion,
                final_login=excluded.final_login, final_login_2=excluded.final_login_2, Regional=excluded.Regional,
                descricao_os=excluded.descricao_os, dslam_projeto=excluded.dslam_projeto, hostname=excluded.hostname,
                primaria=excluded.primaria, caixa=excluded.caixa,
                analisado=CASE WHEN ordens_servico.analisado IS NULL THEN excluded.analisado ELSE ordens_servico.analisado END,
                comentario=CASE WHEN ordens_servico.comentario IS NULL THEN excluded.comentario ELSE ordens_servico.comentario END,
                usuario=CASE WHEN ordens_servico.usuario IS NULL THEN excluded.usuario ELSE ordens_servico.usuario END
        ''', (
            row['so'], row['numero_protocolo'], row['id_planos_usuarios'], row['contrato'], row['tecnico'], row['empresa'],
            row['causa'], row['cidade'], row['descricao_regiao'], row['status_os'], row['cto_latitude'], row['cto_longitude'],
            row['data_agendamento'], row['data_abertura_os'], row['hora_abertura_os'], row['data_finalizacao_os'],
            row['adminversion'], row['final_login'], row['final_login_2'], row['Regional'], row['descricao_os'],
            row['dslam_projeto'], row['hostname'], int(row['primaria']) if pd.notnull(row['primaria']) else None, 
            int(row['caixa']) if pd.notnull(row['caixa']) else None, row.get('analisado', 0), row.get('comentario', ''), row.get('usuario', '')
        ))

    conn_sqlite.commit()
    conn_sqlite.close()

    print("Dados salvos com sucesso no banco de dados SQLite")

@app.route('/atualizar-dados', methods=['GET'])
def atualizar_dados():
    try:
        database_name = inicializar_database()  # Garante que o banco de dados para a data atual seja inicializado e criado se necessário
        executar_consulta_e_salvar_db(database_name)
        return jsonify({"message": "Dados atualizados com sucesso"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/buscar-dados-historicos', methods=['GET'])
def buscar_dados_historicos():
    data = request.args.get('data')
    database_dir = 'public/bancodedados'
    database_name = os.path.join(database_dir, f'database{data}.db')
    print(f"Buscando dados históricos para o banco de dados: {database_name}")
    
    if not os.path.exists(database_name):
        error_message = f"Banco de dados não encontrado: {database_name}"
        print(error_message)
        return jsonify({"error": error_message}), 404
    
    try:
        conn = get_db_connection(database_name)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM ordens_servico WHERE data_abertura_os = ?', (data,))
        rows = cursor.fetchall()
        conn.close()

        dados = [dict(row) for row in rows]
        return jsonify(dados), 200
    except Exception as e:
        print(f"Erro ao buscar dados históricos: {str(e)}")
        return jsonify({"error": str(e)}), 500
    
@app.route('/buscar-sinal', methods=['GET'])
def buscar_sinal():
    login = request.args.get('login')
    try:
        session = criar_sessao_netcore()
        dados_sinal = acessar_cliente(session, login)
        return jsonify(dados_sinal), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/buscar-dados-data', methods=['GET'])
def buscar_dados_data():
    data = request.args.get('data')
    database_dir = 'public/bancodedados'
    database_name = os.path.join(database_dir, f'database{data}.db')
    print(f"Buscando dados históricos para o banco de dados: {database_name}")
    
    if not os.path.exists(database_name):
        error_message = f"Banco de dados não encontrado: {database_name}"
        print(error_message)
        return jsonify({"error": error_message}), 404
    
    try:
        conn = get_db_connection(database_name)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM ordens_servico WHERE data_abertura_os = ?', (data,))
        rows = cursor.fetchall()
        conn.close()

        dados = [dict(row) for row in rows]
        return jsonify(dados), 200
    except Exception as e:
        print(f"Erro ao buscar dados históricos: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/buscar-dados', methods=['GET'])
def buscar_dados():
    try:
        database_name = inicializar_database()
        conn = get_db_connection(database_name)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM ordens_servico')
        rows = cursor.fetchall()
        conn.close()

        dados = [dict(row) for row in rows]
        return jsonify(dados), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/update-event', methods=['POST'])
def update_event():
    data = request.json
    comentario = data.get('comment')
    analisado = data.get('analisado')
    ordem_servico = data.get('ordem_servico')
    usuario = data.get('usuario')

    database_name = inicializar_database()
    conn = get_db_connection(database_name)
    cursor = conn.cursor()

    cursor.execute('''
        UPDATE ordens_servico
        SET comentario = ?, analisado = ?, usuario = ?
        WHERE so = ?
    ''', (comentario, analisado, usuario, ordem_servico))

    cursor.execute('''
        INSERT INTO comentarios (ordem_servico, comentario, usuario)
        VALUES (?, ?, ?)
    ''', (ordem_servico, comentario, usuario))

    conn.commit()
    conn.close()

    socketio.emit('event_updated', {
        'ordem_servico': ordem_servico,
        'comment': comentario,
        'analisado': analisado,
        'usuario': usuario
    })

    return jsonify({"message": "Dados atualizados com sucesso"}), 200

@app.route('/analisar-evento', methods=['POST'])
def analisar_evento():
    data = request.json
    event_id = data.get('event_id')
    analisando = data.get('analisando')
    
    # Emitir evento para todos os clientes conectados via WebSocket
    socketio.emit('evento_analisando', {'event_id': event_id, 'analisando': analisando}, broadcast=True)
    return jsonify({'status': 'success'}), 200

@app.route('/expand-event', methods=['POST'])
def expand_event():
    data = request.json
    event_key = data.get('event_key')

    # Emitir um evento para todos os clientes conectados indicando que o evento está sendo expandido
    socketio.emit('event_expanded', {
        'event_key': event_key
    })

    return jsonify({"message": "Evento expandido com sucesso"}), 200

@app.route('/collapse-event', methods=['POST'])
def collapse_event():
    data = request.json
    event_key = data.get('event_key')

    # Emitir um evento para todos os clientes conectados indicando que o evento foi colapsado
    socketio.emit('event_collapsed', {
        'event_key': event_key
    })

    return jsonify({"message": "Evento colapsado com sucesso"}), 200

@app.route('/username', methods=['GET'])
def get_username():
    username = os.getlogin()
    return jsonify({'username': username})

@app.route('/historico-comentarios', methods=['GET'])
def historico_comentarios():
    ordem_servico = request.args.get('ordem_servico')
    database_name = inicializar_database()
    conn = get_db_connection(database_name)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT comentario, usuario, data_comentario
        FROM comentarios
        WHERE ordem_servico = ?
        ORDER BY data_comentario DESC
    ''', (ordem_servico,))
    rows = cursor.fetchall()
    conn.close()

    historico = [dict(row) for row in rows]
    return jsonify(historico), 200

def autenticar_usuario(username, password):
    login_url = "http://netcore.desktop.localdomain/usuarios/login"
    perfil_url = "http://netcore.desktop.localdomain/usuarios/perfil"

    session = requests.Session()
    response = session.get(login_url)
    csrf_token = response.cookies.get('csrftoken', '')

    payload_login = {
        "csrfmiddlewaretoken": csrf_token,
        "username": username,
        "password": password,
        "next": ""
    }

    login_response = session.post(login_url, data=payload_login, headers={"Referer": login_url})
    if login_response.status_code == 200:
        perfil_response = session.get(perfil_url)
        if perfil_response.status_code == 200:
            return username, True
        else:
            return None, False
    else:
        return None, False

@app.route('/authenticate', methods=['POST'])
def authenticate():
    data = request.json
    username = data.get('username')
    password = data.get('password')

    if not username or not password:
        return jsonify({'error': 'Username and password are required'}), 400

    user, authenticated = autenticar_usuario(username, password)
    if authenticated:
        return jsonify({'username': user}), 200
    else:
        return jsonify({'error': 'Invalid credentials'}), 401

if __name__ == "__main__":
    socketio.run(app, host='172.26.13.52', port=3002)