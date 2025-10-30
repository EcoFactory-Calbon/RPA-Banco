import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# --- Funções de DB (Mantidas, com pequenas melhorias de logging) ---

def _get_data(conn, query, table_name):
    """Obtém dados e colunas do banco de dados de origem."""
    cursor = conn.cursor()
    try:
        print(f"DEBUG: Executando query no DB de origem ({table_name}): {query}")
        cursor.execute(query)
        dados = cursor.fetchall()
        colunas = [desc[0] for desc in cursor.description]
        print(f"DEBUG: Query OK. Encontradas {len(dados)} linhas em {table_name}.")
        return dados, colunas
    except Exception as e:
        print(f"ERRO: ao pegar dados da tabela {table_name}: {e}")
        return [], []
    finally:
        cursor.close()

def _insert_data(conn, data, table_name, columns):
    """Insere/Atualiza dados no banco de dados de destino (UPSERT)."""
    cursor = conn.cursor()
    
    try:
        # Obter a coluna de chave primária para a cláusula ON CONFLICT
        cursor.execute(f"""
            SELECT kcu.column_name 
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = '{table_name}'
              AND tc.constraint_type = 'PRIMARY KEY'
        """)
        
        pk_fetch = cursor.fetchone()
        if not pk_fetch:
             raise ValueError(f"Tabela {table_name} não tem Chave Primária para UPSERT.")
            
        pk_column = pk_fetch[0]
        
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != pk_column])
        
        print(f"DEBUG: Iniciando UPSERT em {table_name} (PK: {pk_column}) para {len(data)} linhas.")
        for linha in data:
            cursor.execute(
                f"""INSERT INTO {table_name} ({columns_str}) 
                VALUES ({placeholders})
                ON CONFLICT ({pk_column}) DO UPDATE 
                SET {update_set}""", 
                linha
            )
        conn.commit()
        print(f"DEBUG: {len(data)} linhas inseridas/atualizadas em {table_name}.")
    except Exception as e:
        print(f"ERRO: Ao inserir/atualizar dados na tabela {table_name}: {e}")
        conn.rollback()
    finally:
        cursor.close()

def _get_table_hash(conn, table):
    """Calcula o hash do conteúdo da tabela."""
    cursor = conn.cursor()
    try:
        # ORDER BY 1 garante que a ordem das linhas não afete o hash
        query = f"SELECT md5(string_agg(t::text, '')) FROM (SELECT * FROM {table} ORDER BY 1) t"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        return result
    except Exception as e:
        # Se a tabela estiver vazia, ou não existir, o hash pode retornar None
        print(f"AVISO: Não foi possível calcular hash para {table}: {e}")
        return None
    finally:
        cursor.close()

def _get_db_hashes(conn, sync_tables):
    """Obtém hashes de todas as tabelas configuradas no banco."""
    return {table: _get_table_hash(conn, table) for table in sync_tables if table}


# --- Conexão e Lógica de Sincronização INVERTIDA ---

# DB1 (DESTINO - bancosegundoano)
db1_host = os.getenv("DB1_HOST")
db1_name = os.getenv("DB1_NAME")
db1_user = os.getenv("DB1_USER")
db1_pass = os.getenv("DB1_PASS")
db1_port = os.getenv("DB1_PORT")

print(f"INFO: Tentando conectar DB1 (DESTINO): Host={db1_host}, DB={db1_name}, User={db1_user}, Port={db1_port}")
conn_1 = psycopg2.connect(
    host=db1_host, database=db1_name, user=db1_user, password=db1_pass, port=db1_port
    )
print("INFO: Conexão DB1 (DESTINO) estabelecida com sucesso.")

# DB2 (ORIGEM - bancoPrimeiro)
db2_host = os.getenv("DB2_HOST")
db2_name = os.getenv("DB2_NAME")
db2_user = os.getenv("DB2_USER")
db2_pass = os.getenv("DB2_PASS")
db2_port = os.getenv("DB2_PORT")

print(f"INFO: Tentando conectar DB2 (ORIGEM): Host={db2_host}, DB={db2_name}, User={db2_user}, Port={db2_port}")
conn_2 = psycopg2.connect(
    host=db2_host, database=db2_name, user=db2_user, password=db2_pass, port=db2_port
    )
print("INFO: Conexão DB2 (ORIGEM) estabelecida com sucesso.")

sync_tables_raw = os.getenv("SYNC_TABLES")
if not sync_tables_raw:
    print("ERRO CRÍTICO: Variável de ambiente SYNC_TABLES está vazia.")
    exit(1)
sync_tables = [t.strip() for t in sync_tables_raw.split(",") if t.strip()]
print(f"INFO: Tabelas configuradas para sincronização: {sync_tables}")

# --- Lógica Principal: LER do DB2 e ESCREVER no DB1 ---
print("-" * 50)
try:
    # Obtemos os hashes
    # O hash de ORIGEM (DB2) é o que queremos ter
    hashes_db2_origem = _get_db_hashes(conn_2, sync_tables)
    # O hash de DESTINO (DB1) é o que vamos checar
    hashes_db1_destino = _get_db_hashes(conn_1, sync_tables)

    # Iteramos sobre o hash da ORIGEM (DB2)
    for table, hash2_origem in hashes_db2_origem.items():
        hash1_destino = hashes_db1_destino.get(table)
        print(f"\nINFO: Tabela '{table}': Hash ORIGEM (DB2)={hash2_origem}, Hash DESTINO (DB1)={hash1_destino}")
        
        # Se os hashes forem diferentes, sincronizamos
        if hash2_origem != hash1_destino:
            print(f"INFO: Tabela {table} está diferente, sincronizando...")
            
            # LER os dados da ORIGEM (conn_2)
            data, columns = _get_data(conn_2, f"SELECT * FROM {table}", table)
            
            if data:
                # INSERIR os dados no DESTINO (conn_1)
                _insert_data(conn_1, data, table, columns)
                print(f"SUCESSO: Tabela {table} sincronizada com {len(data)} linhas no DB1 (DESTINO)!")
            else:
                print(f"AVISO: Tabela {table} **VAZIA** no banco de origem (DB2). Nenhuma sincronização feita.")
        else:
            print(f"INFO: Tabela {table} já está atualizada no DESTINO. Hash é o mesmo.")
finally:
    print("-" * 50)
    print("INFO: Fechando conexões.")
    conn_1.close()
    conn_2.close()

    