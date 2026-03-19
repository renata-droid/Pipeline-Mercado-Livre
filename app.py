import streamlit as st
import os
from vendas import executar_vendas
from datetime import date
from financeiro2 import executar_financeiro
from historico_itens_diario import executar_historico
from gerar_relatorio import executar_ads
from merge_ads_financeiro import executar_merge

st.set_page_config(page_title="Pipeline Mercado Livre")

st.markdown("""
    <style>
        .stApp {
            background: linear-gradient(135deg, #0e1117, #1a1f2b);
            color: white;
        }

        h1, h2, h3, h4, h5, h6, label {
            color: white !important;
        }

        /* BOTÃO PRINCIPAL */
        .stButton > button {
            background-color: #6C5CE7;
            color: white;
            border-radius: 10px;
            border: none;
            padding: 10px 16px;
            font-weight: 500;
        }

        .stButton > button:hover {
            background-color: #5A4BD1;
            color: white;
        }

        /* BOTÃO DOWNLOAD */
        .stDownloadButton > button {
            background-color: #00C896;
            color: white;
            border-radius: 10px;
            border: none;
            padding: 10px 16px;
            font-weight: 500;
        }

        .stDownloadButton > button:hover {
            background-color: #00A87E;
            color: white;
        }

        /* INPUT */
        .stDateInput input {
            background-color: #1f2937;
            color: white;
            border-radius: 8px;
        }

    </style>
""", unsafe_allow_html=True)

col1, col2 = st.columns([3, 8])

with col1:
    st.image("mercadolivre_logo.png", width=280)

with col2:
    st.markdown("<h2 style='margin-top:20px; white-space: nowrap;'>Extração de Vendas - Mercado Livre</h2>", unsafe_allow_html=True)

# seletor de data
data = st.date_input("Selecione a data", value=date.today())

# botão de execução
if st.button("Executar extração"):

    data_str = str(data)

    st.info(f"Rodando vendas para {data_str}...")

    try:
        arquivo = executar_vendas(data_str)

        st.success("Vendas finalizado...")

        #2. financeiro 
        arquivo_financeiro = executar_financeiro(data_str)
        st.info("Financeiro finalizado...")
        

        # 3. histórico
        executar_historico(data_str)
        st.info("Histórico gerado...")

        # 4. ads
        arquivo_ads = executar_ads(data_str)
        st.info("Ads finalizado...")

        # 5. merge final
        arquivo_final = executar_merge(data_str)
        st.info("Consolidado final gerado...")

        # caminho consolidado
        caminho_final = os.path.join("data", "consolidado_diario", arquivo_final)

        with open(caminho_final, "rb") as f:
            st.download_button(
                label="📥 Baixar relatório final",
                data=f.read(),
                file_name=arquivo_final,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        st.write(f"Arquivo gerado: {arquivo_financeiro}")
        st.write(f"Ads: {arquivo_ads}")
        st.success("Processo finalizado!")
    except Exception as e:
        st.error(f"Erro: {e}")