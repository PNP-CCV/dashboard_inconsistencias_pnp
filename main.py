import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import duckdb
from datetime import date

# Function to create a connection to the database
def create_connection(db_path='db.duckdb'):
    try:
        conn = duckdb.connect(database=db_path)
        return conn
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return None

# Function to create the table if it does not exist
def create_table(conn):
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
        st.error(f"Erro ao criar a tabela: {e}")

# Function to insert data into the table from a CSV file
def insert_data_if_needed(conn, csv_path='dados.csv'):
    try:
        last_processing_date = conn.execute("SELECT MAX(\"Data do Processamento\") FROM pnp_data").fetchdf().values[0][0]
        if last_processing_date != pd.to_datetime('today').date():
            conn.execute(f"""
            INSERT INTO pnp_data
                SELECT *, current_date as "Data do Processamento" FROM read_csv('{csv_path}');
            """)
    except Exception as e:
        st.error(f"Erro ao inserir dados: {e}")

# Function to query the data for the last processing date
def query_data(conn):
    try:
        df = conn.execute("""
            SELECT * FROM pnp_data WHERE "Data do Processamento" = (SELECT MAX("Data do Processamento") FROM pnp_data)
        """).fetchdf()
        return df
    except Exception as e:
        st.error(f"Erro ao consultar dados: {e}")
        return pd.DataFrame()

# Function to process the data
def process_data(df):
    try:
        df_grouped = df.groupby(['Data do Processamento', 'Instituição', 'Unidade', 'Escopo da Inconsistência', 'Situação da Inconsistência'], as_index=False)['Total de Inconsistências'].sum()
        df_grouped['Data do Processamento'] = pd.to_datetime(df_grouped['Data do Processamento'])
        df_grouped['Data do Processamento'] = df_grouped['Data do Processamento'].dt.strftime('%d/%m/%Y')

        return df_grouped
    except Exception as e:
        st.error(f"Erro ao processar dados: {e}")
        return pd.DataFrame()

# Function to close the connection
def close_connection(conn):
    try:
        conn.close()
    except Exception as e:
        st.error(f"Erro ao fechar a conexão: {e}")

# Main function
def main():
    conn = create_connection()
    if conn is None:
        return

    create_table(conn)
    insert_data_if_needed(conn)
    df = query_data(conn)
    df_grouped = process_data(df)
    close_connection(conn)

    # Streamlit interface
    st.set_page_config(
        page_title="PNP 2025 - Painel Acompanhamento",
        layout="wide"
    )

    # Title and description
    st.image('logo.svg', width=300)
    st.title("Painel Acompanhamento")
    st.write("Este painel permite acompanhar o andamento das correções de inconsistências da PNP.")

    # Sidebar to filter the data
    st.sidebar.title("Filtros")

    # Include "Todos" as an option for each filter
    instituicoes = ['Todos'] + df['Instituição'].unique().tolist()
    unidades = ['Todos'] + df['Unidade'].unique().tolist()
    escopos = ['Todos'] + df['Escopo da Inconsistência'].unique().tolist()

    # Filter for the processing date
    max_data_processamento = pd.to_datetime(df['Data do Processamento']).max().strftime('%Y-%m-%d')
    min_data_processamento = pd.to_datetime(df['Data do Processamento']).min().strftime('%Y-%m-%d')
    data_processamento = st.sidebar.date_input("Data do Processamento", max_data_processamento, max_value=max_data_processamento, min_value=min_data_processamento, format='DD/MM/YYYY')

    instituicoes = st.sidebar.multiselect("Selecione a Instituição", instituicoes, default='Todos')

    # Filter the units based on the selected institution
    if 'Todos' not in instituicoes:
        unidades = ['Todos'] + df[df['Instituição'].isin(instituicoes)]['Unidade'].unique().tolist()
    else:
        unidades = ['Todos'] + df['Unidade'].unique().tolist()
        
    unidades = st.sidebar.multiselect("Selecione a Unidade", unidades, default='Todos')
    escopos = st.sidebar.multiselect("Selecione o Escopo", escopos, default='Todos')

    # Show raw data and allow user to filter
    st.write("### Dados de Inconsistências")

    if 'Todos' in instituicoes:
        instituicoes = df['Instituição'].unique().tolist()
    if 'Todos' in unidades:
        unidades = df['Unidade'].unique().tolist()
    if 'Todos' in escopos:
        escopos = df['Escopo da Inconsistência'].unique().tolist()

    # Show the filtered data if all filter is selected
    if instituicoes and unidades and escopos:
        filtered_data = df_grouped[
            df_grouped['Instituição'].isin(instituicoes) &
            df_grouped['Unidade'].isin(unidades) &
            df_grouped['Escopo da Inconsistência'].isin(escopos) &
            df_grouped['Data do Processamento'].isin([data_processamento.strftime('%d/%m/%Y')])
        ]

        # Display the filtered data
        st.dataframe(filtered_data, hide_index=True)

if __name__ == "__main__":
    main()