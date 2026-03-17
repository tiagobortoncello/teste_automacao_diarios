import streamlit as st
from datetime import datetime
import pandas as pd
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re

# ===== CONFIG =====
PLANILHA_URL = "https://docs.google.com/spreadsheets/d/1XQ8VMo_O5i8KLQWmb_s4xrBuisUQUgdmgQw5xoCu-ms"

# ===== GOOGLE AUTH =====
def conectar_gsheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "credenciais.json", scope
    )

    client = gspread.authorize(creds)
    return client.open_by_url(PLANILHA_URL).sheet1


# ===== DATA =====
def preparar_datas(data_str):
    dt = datetime.strptime(data_str, "%d/%m/%Y")

    return {
        "yyyy": dt.strftime("%Y"),
        "mm": dt.strftime("%m"),
        "dd": dt.strftime("%d"),
        "yyyymmdd": dt.strftime("%Y%m%d"),
        "iso_exec": dt.strftime("%Y-%m-%dT06:00:00.000Z"),
        "data_planilha": dt.strftime("%Y-%m-%d 00:00:00")
    }


# ===== URLS =====
def montar_urls(d):
    return {
        "executivo_html": f"https://www.jornalminasgerais.mg.gov.br/edicao-do-dia?dados=%7B%22dataPublicacaoSelecionada%22:%22{d['iso_exec']}%22%7D",
        "legislativo": f"https://diariolegislativo.almg.gov.br/{d['yyyy']}/L{d['yyyymmdd']}.pdf",
        "administrativo": f"https://intra.almg.gov.br/export/sites/default/acontece/diario-administrativo/arquivos/{d['yyyy']}/{d['mm']}/L{d['yyyymmdd']}.pdf"
    }


# ===== DOWNLOAD =====
def baixar(url):
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content


def extrair_pdf_exec(html):
    match = re.search(r'https://.*?\.pdf', html)
    return match.group(0) if match else None


def baixar_exec(url):
    r = requests.get(url)
    pdf_url = extrair_pdf_exec(r.text)
    return baixar(pdf_url)


# ===== ESCRITA NA PLANILHA =====
def encontrar_linha_data(sheet, data_str):
    valores = sheet.col_values(1)

    for i, v in enumerate(valores):
        if v.strip() == data_str:
            return i + 1

    return None


def escrever_normas(sheet, linha_base, df):
    if df.empty:
        return

    dados = []
    for _, row in df.iterrows():
        dados.append([
            "",  # mantém estrutura
            row.get("Página", ""),
            row.get("Coluna", ""),
            row.get("Sanção", ""),
            row.get("Sigla", ""),
            row.get("Número", "")
        ])

    sheet.update(
        f"B{linha_base+1}:G{linha_base+len(dados)}",
        dados
    )


# ===== APP =====
st.title("📄 Diário MG → Automação")

data = st.text_input("Data (DD/MM/AAAA)", "17/03/2026")

if st.button("Processar"):

    d = preparar_datas(data)
    urls = montar_urls(d)

    st.write("🔎 Buscando dados...")

    # ===== EXECUTIVO =====
    try:
        pdf_exec = baixar_exec(urls["executivo_html"])
        exec_proc = ExecutiveProcessor(pdf_exec)
        df_exec = exec_proc.process_pdf()
    except Exception as e:
        st.error(f"Erro Executivo: {e}")
        df_exec = pd.DataFrame()

    # ===== LEGISLATIVO =====
    try:
        pdf_leg = baixar(urls["legislativo"])
        leg_proc = LegislativeProcessor(pdf_leg)
        df_leg = leg_proc.process_all()["Normas"]
    except Exception as e:
        st.error(f"Erro Legislativo: {e}")
        df_leg = pd.DataFrame()

    # ===== ADMIN =====
    try:
        pdf_adm = baixar(urls["administrativo"])
        adm_proc = AdministrativeProcessor(pdf_adm)
        df_adm = adm_proc.process_pdf()
    except Exception as e:
        st.warning("Adm provavelmente não acessível no cloud")
        df_adm = pd.DataFrame()

    # ===== GOOGLE SHEETS =====
    sheet = conectar_gsheet()

    linha = encontrar_linha_data(sheet, d["data_planilha"])

    if not linha:
        st.error("Data não encontrada na planilha")
    else:
        st.success(f"Atualizando linha {linha}")

        escrever_normas(sheet, linha, df_exec)

    st.success("Processo finalizado 🚀")
