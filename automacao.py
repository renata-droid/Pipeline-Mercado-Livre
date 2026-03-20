from datetime import datetime, timedelta
import os
from vendas import executar_vendas
from financeiro2 import executar_financeiro
from historico_itens_diario import executar_historico
from gerar_relatorio import executar_ads
from merge_ads_financeiro import executar_merge

# =========================
# DATA D-1
# =========================
data = datetime.today() - timedelta(days=1)
data_str = data.strftime('%Y-%m-%d')

print(f"Rodando automação para: {data_str}")

# =========================
# EXECUÇÃO
# =========================
executar_vendas(data_str)
executar_financeiro(data_str)
executar_historico(data_str)
executar_ads(data_str)

df = executar_merge(data_str)

# =========================
# SALVAR EXCEL
# =========================
caminho = os.path.join("data", "consolidado_diario")
os.makedirs(caminho, exist_ok=True)

arquivo = os.path.join(caminho, f"consolidado_{data_str}.xlsx")

df.to_excel(arquivo, index=False)

print(f"Arquivo salvo em: {arquivo}")

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

gauth = GoogleAuth()
gauth.LocalWebserverAuth()

drive = GoogleDrive(gauth)

file = drive.CreateFile({
    'title': f'consolidado_{data_str}.xlsx',
    'parents': [{'id': '1daZRFwCXWOJ4QHjP9k5X2DoEPiExXHdA'}]
})

file.SetContentFile(arquivo)
file.Upload()

print("Arquivo enviado para o Google Drive com sucesso!")