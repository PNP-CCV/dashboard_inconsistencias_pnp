import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import plotly.graph_objects as go
from datetime import timedelta

# Function to create a connection to the database
def create_connection(db_path='db.duckdb'):
    try:
        conn = duckdb.connect(database=db_path)
        return conn
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return None

# Function to query the data for the last processing date
def query_data(conn):
    try:
        df = conn.execute("""
            SELECT * FROM pnp_data
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
        # Mantém o formato de data para o filtro, mas preserva o objeto datetime para o gráfico de linha do tempo
        df_grouped['Data Formatada'] = df_grouped['Data do Processamento'].dt.strftime('%d/%m/%Y')
        
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

# Function to create timeline chart
def create_timeline_chart(data, colors, escopo_filter=None, instituicao_filter=None, unidade_filter=None):
    if data.empty:
        return go.Figure()
    
    # Filtrar dados conforme necessário
    filtered_data = data.copy()
    if escopo_filter:
        filtered_data = filtered_data[filtered_data['Escopo da Inconsistência'] == escopo_filter]
    if instituicao_filter and instituicao_filter != 'Todos':
        filtered_data = filtered_data[filtered_data['Instituição'] == instituicao_filter]
    if unidade_filter and unidade_filter != 'Todos':
        filtered_data = filtered_data[filtered_data['Unidade'] == unidade_filter]
        
    if filtered_data.empty:
        return go.Figure()
    
    # Agrupar por data e situação
    timeline_df = filtered_data.groupby(['Data do Processamento', 'Situação da Inconsistência'], as_index=False)['Total de Inconsistências'].sum()
    
    # Se houver apenas uma data, criar um ponto adicional para mostrar tendência
    dates = timeline_df['Data do Processamento'].unique()
    if len(dates) == 1:
        # Criar um ponto fictício para o dia anterior com os mesmos valores
        temp_df = timeline_df.copy()
        temp_df['Data do Processamento'] = temp_df['Data do Processamento'] - timedelta(days=1)
        timeline_df = pd.concat([temp_df, timeline_df])
    
    # Criar gráfico de linha
    fig = go.Figure()
    
    # Adicionar uma linha para cada situação
    for situacao in ['Inconsistente RA', 'Alterado RA', 'Validado RA', 'Inconsistente PI', 'Alterado PI', 'Validado PI', 'Alterado RE', 'Validado RE']:
        situacao_data = timeline_df[timeline_df['Situação da Inconsistência'] == situacao]
        if not situacao_data.empty:
            fig.add_trace(go.Scatter(
                x=situacao_data['Data do Processamento'],
                y=situacao_data['Total de Inconsistências'],
                mode='lines+markers',
                name=situacao,
                line=dict(color=colors.get(situacao)),
                hovertemplate='%{y:,.0f} inconsistências<br>%{x|%d/%m/%Y}<extra></extra>'
            ))
    
    # Título do gráfico
    title = "Evolução das Inconsistências ao Longo do Tempo"
    if escopo_filter:
        title += f" - {escopo_filter}"
    if instituicao_filter and instituicao_filter != 'Todos':
        title += f" - {instituicao_filter}"
    if unidade_filter and unidade_filter != 'Todos':
        title += f" - {unidade_filter}"
    
    # Configurações do layout
    fig.update_layout(
        title=title,
        xaxis_title="Data do Processamento",
        yaxis_title="Total de Inconsistências",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0
        ),
        xaxis=dict(
            tickformat='%d/%m/%Y',
            tickmode='auto',
            nticks=10
        )
    )
    
    return fig

# Function to create progress chart
def create_progress_chart(data, entity_type, colors, escopo_filter=None):
    if data.empty:
        return go.Figure()
    
    # Filtra por escopo se especificado
    if escopo_filter:
        data = data[data['Escopo da Inconsistência'] == escopo_filter]
        
    if data.empty:
        return go.Figure()
    
    # Cria um pivot com a situação da inconsistência
    pivot_df = data.pivot_table(
        index=[entity_type],
        columns='Situação da Inconsistência',
        values='Total de Inconsistências',
        aggfunc='sum'
    ).fillna(0).reset_index()
    
    # Garante que todas as colunas existam
    for col in ['Inconsistente RA', 'Alterado RA', 'Validado RA', 'Inconsistente PI', 'Alterado PI', 'Validado PI', 'Alterado RE', 'Validado RE']:
        if col not in pivot_df.columns:
            pivot_df[col] = 0
            
    # Calcula o total e o percentual de cada situação
    pivot_df['Total'] = pivot_df['Inconsistente RA'] + pivot_df['Alterado RA'] + pivot_df['Validado RA'] + pivot_df['Inconsistente PI'] + pivot_df['Alterado PI'] + pivot_df['Validado PI'] + pivot_df['Alterado RE'] + pivot_df['Validado RE']
    pivot_df['% Inconsistente RA'] = (pivot_df['Inconsistente RA'] / pivot_df['Total'] * 100).round(1)
    pivot_df['% Alterado RA'] = (pivot_df['Alterado RA'] / pivot_df['Total'] * 100).round(1)
    pivot_df['% Validado RA'] = (pivot_df['Validado RA'] / pivot_df['Total'] * 100).round(1)
    pivot_df['% Inconsistente PI'] = (pivot_df['Inconsistente PI'] / pivot_df['Total'] * 100).round(1)
    pivot_df['% Alterado PI'] = (pivot_df['Alterado PI'] / pivot_df['Total'] * 100).round(1)
    pivot_df['% Validado PI'] = (pivot_df['Validado PI'] / pivot_df['Total'] * 100).round(1)
    pivot_df['% Alterado RE'] = (pivot_df['Alterado RE'] / pivot_df['Total'] * 100).round(1)
    pivot_df['% Validado RE'] = (pivot_df['Validado RE'] / pivot_df['Total'] * 100).round(1)
        
    # Ordena por total (maior para o menor)
    pivot_df = pivot_df.sort_values('Total', ascending=False)
    
    # Cria gráfico de barras horizontais empilhadas
    fig = go.Figure()
    
    for col in ['Inconsistente RA', 'Alterado RA', 'Validado RA', 'Inconsistente PI', 'Alterado PI', 'Validado PI', 'Alterado RE', 'Validado RE']:

        fig.add_trace(go.Bar(
        y=pivot_df[entity_type],
        x=pivot_df[f'% {col}'],
        name=col,
        orientation='h',
        marker=dict(color=colors[col]),
        text=pivot_df['% ' + col].apply(lambda x: f"{x}%" if x > 5 else ""),
        textposition='auto',
        hovertemplate=col + ": %{x:.1f}%<extra></extra>"
    ))

   

    # Título específico baseado no filtro de escopo
    title = f"Progresso por {entity_type}"
    if escopo_filter:
        title += f" - Escopo: {escopo_filter}"
    
    # Configurações do layout
    fig.update_layout(
        title=title,
        barmode='stack',
        height=max(350, len(pivot_df) * 30),  # Ajusta altura baseado no número de itens
        margin=dict(l=0, r=0, t=40, b=0),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0
        ),
        xaxis=dict(title="Percentual (%)", ticksuffix="%"),
        hovermode="closest"
    )
    
    return fig

# Function to create summary cards
def create_summary_cards(data, escopo_filter=None):
    if data.empty:
        return
    
    # Filtra por escopo se especificado
    if escopo_filter:
        data = data[data['Escopo da Inconsistência'] == escopo_filter]
        
    if data.empty:
        return
    
    total_inconsistente_ra = data[data['Situação da Inconsistência'] == 'Inconsistente RA']['Total de Inconsistências'].sum()
    total_alterado_ra = data[data['Situação da Inconsistência'] == 'Alterado RA']['Total de Inconsistências'].sum()
    total_validado_ra = data[data['Situação da Inconsistência'] == 'Validado RA']['Total de Inconsistências'].sum()
    total_inconsistente_pi = data[data['Situação da Inconsistência'] == 'Inconsistente PI']['Total de Inconsistências'].sum()
    total_alterado_pi = data[data['Situação da Inconsistência'] == 'Alterado PI']['Total de Inconsistências'].sum()
    total_validado_pi = data[data['Situação da Inconsistência'] == 'Validado PI']['Total de Inconsistências'].sum()
    total_validado_re = data[data['Situação da Inconsistência'] == 'Validado RE']['Total de Inconsistências'].sum()
    total_geral = total_alterado_ra + total_inconsistente_ra + total_alterado_pi + total_validado_re + total_validado_ra + total_validado_pi
    
    # Cálculo de percentuais
    pct_inconsistente_ra = (total_inconsistente_ra / total_geral * 100) if total_geral > 0 else 0
    pct_alterado_ra = (total_alterado_ra / total_geral * 100) if total_geral > 0 else 0
    pct_validado_ra = (total_validado_ra / total_geral * 100) if total_geral > 0 else 0
    pct_inconsistente_pi = (total_inconsistente_pi / total_geral * 100) if total_geral > 0 else 0
    pct_alterado_pi = (total_alterado_pi / total_geral * 100) if total_geral > 0 else 0
    pct_validado_pi = (total_validado_pi / total_geral * 100) if total_geral > 0 else 0
    pct_validado_re = (total_validado_re / total_geral * 100) if total_geral > 0 else 0
    
    # Cria colunas para os cards
    col1, col2, col3, col4, col5, col6, col7, col8 = st.columns(8)
    
    titulo = "Total de Inconsistências"
    if escopo_filter:
        titulo += f" ({escopo_filter})"
    
    with col1:
        st.metric(titulo, f"{total_geral:,}".replace(",", "."))
    
    with col2:
        st.metric("Inconsistente RA", f"{total_inconsistente_ra:,}".replace(",", "."), f"{pct_inconsistente_ra:.1f}%", delta_color="inverse")
        
    with col3:
        st.metric("Alterado RA", f"{total_alterado_ra:,}".replace(",", "."), f"{pct_alterado_ra:.1f}%")

    with col4:
        st.metric("Validado RA", f"{total_validado_ra:,}".replace(",", "."), f"{pct_validado_ra:.1f}%")
        
    with col5:
        st.metric("Inconsistente PI", f"{total_inconsistente_pi:,}".replace(",", "."), f"{pct_inconsistente_pi:.1f}%", delta_color="inverse")
    
    with col6:
        st.metric("Alterado PI", f"{total_alterado_pi:,}".replace(",", "."), f"{pct_alterado_pi:.1f}%")
    
    with col7:
        st.metric("Validado PI", f"{total_validado_pi:,}".replace(",", "."), f"{pct_validado_pi:.1f}%")
    
    with col8:
        st.metric("Validado RE", f"{total_validado_re:,}".replace(",", "."), f"{pct_validado_re:.1f}%")

# Main function
def main():
    conn = create_connection()
    if conn is None:
        return

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
    instituicoes = ['Todos'] + sorted(df['Instituição'].unique().tolist())
    unidades = ['Todos'] + sorted(df['Unidade'].unique().tolist())
    escopos = sorted(df['Escopo da Inconsistência'].unique().tolist())

    # Filter for the processing date range
    min_date = pd.to_datetime(df['Data do Processamento']).min().date()
    max_date = pd.to_datetime(df['Data do Processamento']).max().date()
    
    # Por padrão, considere o último mês de dados
    default_start = max_date - timedelta(days=30)
    if default_start < min_date:
        default_start = min_date
        
    col1, col2 = st.sidebar.columns(2)
    with col1:
        data_inicial = st.date_input("Data Inicial", default_start, min_value=min_date, max_value=max_date)
    with col2:
        data_final = st.date_input("Data Final", max_date, min_value=min_date, max_value=max_date)

    instituicoes_selecionadas = st.sidebar.multiselect("Selecione a Instituição", instituicoes, default='Todos')

    # Filter the units based on the selected institution
    if 'Todos' not in instituicoes_selecionadas and instituicoes_selecionadas:
        unidades = ['Todos'] + sorted(df[df['Instituição'].isin(instituicoes_selecionadas)]['Unidade'].unique().tolist())
    else:
        unidades = ['Todos'] + sorted(df['Unidade'].unique().tolist())
        
    unidades_selecionadas = st.sidebar.multiselect("Selecione a Unidade", unidades, default='Todos')

    # Apply filters
    if 'Todos' in instituicoes_selecionadas or not instituicoes_selecionadas:
        instituicoes_selecionadas = df['Instituição'].unique().tolist()
    if 'Todos' in unidades_selecionadas or not unidades_selecionadas:
        unidades_selecionadas = df['Unidade'].unique().tolist()

    # Filter data by institution, unit and date range
    filtered_data = df_grouped[
        df_grouped['Instituição'].isin(instituicoes_selecionadas) &
        df_grouped['Unidade'].isin(unidades_selecionadas) &
        (df_grouped['Data do Processamento'] >= pd.Timestamp(data_inicial)) &
        (df_grouped['Data do Processamento'] <= pd.Timestamp(data_final))
    ]
    
    if filtered_data.empty:
        st.warning("Não há dados para a combinação de filtros selecionada.")
        return
    
    # Dict com as cores a serem usadas para cada tipo de inconsistência
    colors = {
        'Inconsistente RA': '#660033',
        'Alterado RA': '#cc3333',
        'Validado RA': '#ff6666',
        'Inconsistente PI': '#1a5599', 
        'Alterado PI': '#3377bb', 
        'Validado PI': '#66aadd', 
        'Alterado RE': '#66cc77', 
        'Validado RE': '#228833'   
    }
    
    # Criar abas para diferentes escopos de inconsistência
    tabs = st.tabs(["Visão Geral"] + escopos)
    
    # Tab Visão Geral
    with tabs[0]:
        st.write("## Resumo Geral de Inconsistências")
        create_summary_cards(filtered_data)
        
        # Gráfico de linha do tempo para acompanhar a evolução
        st.write("## Evolução das Inconsistências")
        fig_timeline = create_timeline_chart(filtered_data, colors)
        st.plotly_chart(fig_timeline, use_container_width=True)
        
        # Visualização gráfica do progresso por instituição
        st.write("## Progresso por Instituição")
        fig_instituicao = create_progress_chart(filtered_data, 'Instituição', colors)
        st.plotly_chart(fig_instituicao, use_container_width=True)
        
        # Se apenas uma instituição estiver selecionada, mostrar progresso por unidade
        if len(instituicoes_selecionadas) == 1:
            st.write("## Progresso por Unidade")
            fig_unidade = create_progress_chart(filtered_data, 'Unidade', colors)
            st.plotly_chart(fig_unidade, use_container_width=True)
        
        # Mostrar tabela detalhada
        with st.expander("Dados Detalhados"):
            display_df = filtered_data.copy()
            display_df['Data do Processamento'] = display_df['Data Formatada']
            display_df = display_df.drop('Data Formatada', axis=1)
            st.dataframe(display_df, hide_index=True)
    
    # Tab para cada escopo de inconsistência
    for i, escopo in enumerate(escopos, 1):
        with tabs[i]:
            st.write(f"## Resumo de Inconsistências - {escopo}")
            escopo_data = filtered_data[filtered_data['Escopo da Inconsistência'] == escopo]
            
            if escopo_data.empty:
                st.info(f"Não há dados para o escopo {escopo} com os filtros selecionados.")
                continue
                
            create_summary_cards(filtered_data, escopo)
            
            # Gráfico de linha do tempo específico para este escopo
            st.write(f"## Evolução das Inconsistências - {escopo}")
            fig_timeline_escopo = create_timeline_chart(filtered_data, colors, escopo)
            st.plotly_chart(fig_timeline_escopo, use_container_width=True)
            
            # Visualização gráfica do progresso por instituição para este escopo
            st.write(f"## Progresso por Instituição - {escopo}")
            fig_instituicao = create_progress_chart(filtered_data, 'Instituição', colors, escopo)
            st.plotly_chart(fig_instituicao, use_container_width=True)
            
            # Se apenas uma instituição estiver selecionada, mostrar progresso por unidade para este escopo
            if len(instituicoes_selecionadas) == 1:
                st.write(f"## Progresso por Unidade - {escopo}")
                fig_unidade = create_progress_chart(filtered_data, 'Unidade', colors, escopo)
                st.plotly_chart(fig_unidade, use_container_width=True)
                
                # Se uma instituição e uma unidade estiverem selecionadas, mostrar gráfico de linha do tempo específico
                if len(unidades_selecionadas) == 1 and unidades_selecionadas[0] != 'Todos':
                    st.write(f"## Evolução na {unidades_selecionadas[0]} - {escopo}")
                    fig_timeline_unidade = create_timeline_chart(filtered_data, colors, escopo, 
                                                               instituicoes_selecionadas[0], unidades_selecionadas[0])
                    st.plotly_chart(fig_timeline_unidade, use_container_width=True)
            
            # Mostrar tabela detalhada para este escopo
            with st.expander(f"Dados Detalhados - {escopo}"):
                display_df = escopo_data.copy()
                display_df['Data do Processamento'] = display_df['Data Formatada']
                display_df = display_df.drop('Data Formatada', axis=1)
                st.dataframe(display_df, hide_index=True)

if __name__ == "__main__":
    main()