import streamlit as st
import os
import pandas as pd
import plotly.express as px
from vendas import executar_vendas
from datetime import date
from financeiro2 import executar_financeiro
from historico_itens_diario import executar_historico
from gerar_relatorio import executar_ads
from merge_ads_financeiro import executar_merge
from io import BytesIO

st.set_page_config(page_title="Pipeline Mercado Livre", layout="wide")
st.empty()

# =========================
# ESTILO
# =========================
st.markdown("""
<style>
.stApp {
    background: linear-gradient(135deg, #0e1117, #1a1f2b);
    color: white;
}

h1, h2, h3, h4, h5, h6, label {
    color: white !important;
}

.stButton > button {
    background-color: #6C5CE7;
    color: white;
    border-radius: 10px;
    padding: 10px 16px;
}

.stDownloadButton > button {
    background-color: #00C896;
    color: white;
    border-radius: 10px;
}

.stDateInput input {
    background-color: #1f2937;
    color: white;
}
div[data-testid="stFormSubmitButton"] button {
    background-color: #6C5CE7 !important;
    color: white !important;
    border-radius: 10px !important;
    padding: 10px 16px !important;
}
</style>
""", unsafe_allow_html=True)

if "logado" not in st.session_state:
    st.session_state.logado = False

if not st.session_state.logado:

    col1, col2, col3 = st.columns([2, 4, 2])

    with col2:
        st.image("mercadolivre_logo.png", width=220)

        st.markdown("""
        <h1 style='text-align:center; font-size:38px; margin-bottom:10px;'>
        Dashboard Mercado Livre
        </h1>
        """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        st.markdown("<h3 style='text-align:center;'>🔐 Login</h3>", unsafe_allow_html=True)

        with st.form("login_form"):

            usuario = st.text_input("Usuário")
            senha = st.text_input("Senha", type="password")

            st.markdown("<br>", unsafe_allow_html=True)

            entrar = st.form_submit_button("Entrar", use_container_width=True)

        if entrar:
            if usuario.strip() == "admin" and senha.strip() == "icommconsultoria":
                st.session_state.logado = True
                st.rerun()
            else:
                st.error("Login inválido")
    st.stop()

# =========================
# HEADER
# =========================
col1, col2 = st.columns([2, 10])

with col1:
    st.image("mercadolivre_logo.png", width=260)

with col2:
    st.markdown("<h1 style='margin-top:10px;'>Dashboard Mercado Livre</h1>", unsafe_allow_html=True)

# =========================
# CONTROLES
# =========================
data = st.date_input("Selecione a data", value=date.today())
executar = st.button("Executar extração")

data_str = str(data)
caminho_final = os.path.join("data", "consolidado_diario", f"consolidado_{data_str}.xlsx")
df = None

# =========================
# EXECUÇÃO PROTEGIDA
# =========================
if executar:
    try:
        with st.spinner("Processando dados..."):
            executar_vendas(data_str)
            executar_financeiro(data_str)
            executar_historico(data_str)
            executar_ads(data_str)
            df=executar_merge(data_str)
        output = BytesIO()
        df.to_excel(output, index=False, engine='openpyxl')
        output.seek(0)

        st.success("Dados atualizados!")

    except Exception as e:
        st.error("Erro ao conectar com a API. Tente novamente.")
        st.text(str(e))

# =========================
# DASHBOARD
# =========================
if executar and df is not None:

    st.markdown("### 📊 Resumo do dia")
    st.markdown("<br>", unsafe_allow_html=True)

    col1, col2, col3, col4, col5 = st.columns(5)

    def formatar(valor):
        return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    def card(titulo, valor):
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #1a1f2b, #2a2f45);
            padding: 20px;
            border-radius: 12px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.3);
        ">
            <div style="color:#9aa4b2; font-size:12px;">{titulo}</div>
            <div style="color:#a855f7; font-size:28px; font-weight:600;">{valor}</div>
        </div>
        """, unsafe_allow_html=True)

    valor_bruto = df['valor_bruto_item'].sum()
    valor_liq = df['valor_liquido'].sum()
    frete = df['frete_regra_calculada'].sum()
    ads = df['ads_rateado'].sum()

    with col1:
        card("Faturamento", formatar(valor_bruto))

    with col2:
        card("Pedidos", df["order_id"].nunique())

    with col3:
        card("Faturamento Líquido", formatar(valor_liq))

    with col4:
        card("Frete", formatar(frete))

    with col5:
        card("Ads", formatar(ads))

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("### 🏆 SKUs mais vendidos")

    top_skus = (
        df.groupby("seller_sku")["quantity"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )

    fig = px.bar(
        top_skus.reset_index(),
        x="seller_sku",
        y="quantity",
        text="quantity"
    )

    fig.update_traces(
        marker_color="#7c3aed",
        textposition="outside"
    )

    fig.update_layout(
        plot_bgcolor="#0e1117",
        paper_bgcolor="#0e1117",
        font_color="white",
        xaxis_title="",
        yaxis_title="",
        title=None
    )

    st.plotly_chart(fig, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # =========================
    # DOWNLOAD
    # =========================
    with open(caminho_final, "rb") as f:
            st.download_button(
                label="📥 Baixar relatório final",
                data=f.read(),
                file_name=f"consolidado_{data_str}.xlsx"
        )