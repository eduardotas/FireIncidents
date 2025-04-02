import requests
import pandas as pd
from sqlalchemy import create_engine

TEMP_FILE = "/data/bronze"

def extract_data():
    """Extrai dados da API de São Francisco e salva em CSV."""
    url = "https://data.sfgov.org/resource/wr8u-xric.json"
    response = requests.get(url)
    data = response.json()

    df = pd.DataFrame(data)
    df.to_csv(TEMP_FILE, index=False)