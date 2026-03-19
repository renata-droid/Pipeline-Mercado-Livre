import requests
import json
import time

CLIENT_ID = "117880451312847"
CLIENT_SECRET = "2kCm1uDT9FYqdIAz8Ak7V4FlrSm2aUj5"
CODE = "TG-694aeb6feed4230001ac79ae-1087616640"
REDIRECT_URI = "https://icommescola.com.br/"

url = "https://api.mercadolibre.com/oauth/token"

payload = {
    "grant_type": "authorization_code",
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "code": CODE,
    "redirect_uri": REDIRECT_URI
}

print("🔄 Enviando requisição para gerar token...")

response = requests.post(url, data=payload)

print("Status:", response.status_code)
print("Resposta:", response.text)

response.raise_for_status()

data = response.json()

token = {
    "access_token": data["access_token"],
    "refresh_token": data["refresh_token"],
    "expires_in": data["expires_in"],
    "created_at": int(time.time())
}

with open("meli_token.json", "w") as f:
    json.dump(token, f, indent=2)

print("✅ Token gerado e salvo com sucesso em meli_token.json")
