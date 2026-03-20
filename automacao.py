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

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

SCOPES = ['https://www.googleapis.com/auth/drive']

SERVICE_ACCOUNT_FILE = 'service_account.json'

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=SCOPES
)

service = build('drive', 'v3', credentials=credentials)

file_metadata = {
    'name': f'consolidado_{data_str}.xlsx',
    'parents': ['1XGcLukCI8uvQWG9T6aAdCYai5xghlI9d']
}

media = MediaFileUpload(
    arquivo,
    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
)

file = service.files().create(
    body=file_metadata,
    media_body=media,
    fields='id',
    supportsAllDrives=True
).execute()

print("Arquivo enviado para o Google Drive com sucesso!")