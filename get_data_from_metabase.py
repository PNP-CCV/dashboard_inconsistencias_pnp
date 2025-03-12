from datetime import datetime
import requests
import pandas as pd
import duckdb

METABASE_URL = 'https://novopnp-mb.mec.gov.br'

def __get_data_from_metabase():
    try:
        headers = {
            'Content-Type': 'application/json',
            'x-api-key': 'mb_stL9qu/yE8NcP4YbyrSBDdUSoJzzpBL/tV7ny8c7YZE='
        }
        response = requests.post(f"{METABASE_URL}/api/card/71/query/csv", headers=headers)
        if response.status_code == 200:
            hoje = datetime.now().strftime('%Y-%m-%d')
            with open(f'data/dados-{hoje}.csv', 'wb') as f:
                f.write(response.content)
        else:
            print(f"Erro ao obter os dados do Metabase: {response.text}")

    except Exception as e:
        print(f"Erro ao conectar ao Metabase: {e}")

def __get_data_from_csv():
    try:
        hoje = datetime.now().strftime('%Y-%m-%d')

        df = pd.read_csv(f'data/dados-{hoje}.csv')
        df.columns = ['Instituição', 'Unidade', 'Escopo da Inconsistência', 'Situação da Inconsistência', 'Total de Inconsistências']
        df.to_csv(f'data/dados-{hoje}.csv', index=False)

        return df

    except Exception as e:
        print(f"Erro ao ler o arquivo CSV: {e}")

def __create_duckdb_connection(db_path='db.duckdb'):
    try:
        conn = duckdb.connect(database=db_path)
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None

def __create_table_in_duckdb(conn):
    try:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS pnp_data (
            "Instituição" VARCHAR,
            "Unidade" VARCHAR,
            "Escopo da Inconsistência" VARCHAR,
            "Situação da Inconsistência" VARCHAR,
            "Total de Inconsistências" INTEGER,
            "Data do Processamento" DATE
        )
        """)
    except Exception as e:
        print(f"Erro ao criar a tabela: {e}")

def __insert_data_if_needed(conn, csv_path='dados.csv'):
    try:
        last_processing_date = conn.execute("SELECT MAX(\"Data do Processamento\") FROM pnp_data").fetchdf().values[0][0]

        if last_processing_date != pd.to_datetime('today').date():
            conn.execute(f"""
            INSERT INTO pnp_data
                SELECT *, current_date as "Data do Processamento" FROM read_csv('{csv_path}');
            """)
    except Exception as e:
        print(f"Erro ao inserir dados: {e}")

def run_pipeline():
    try:
        # Get data from Metabase and save it to a CSV file
        __get_data_from_metabase()
        __get_data_from_csv()

        # Create a connection to the database
        conn = __create_duckdb_connection()
        __create_table_in_duckdb(conn)
       
        today = datetime.now().strftime("%Y-%m-%d")
        __insert_data_if_needed(conn, f'data/dados-{today}.csv')
    except Exception as e:
        print(f"Erro ao executar o pipeline: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    run_pipeline()