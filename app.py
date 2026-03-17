# -*- coding: utf-8 -*-
import streamlit as st
import re
import pandas as pd
import pypdf
import io
import requests
import pdfplumber
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import json
import base64

# =========================
# CONFIG
# =========================
PLANILHA_URL = "https://docs.google.com/spreadsheets/d/1XQ8VMo_O5i8KLQWmb_s4xrBuisUQUgdmgQw5xoCu-ms"

# =========================
# GOOGLE SHEETS
# =========================
def conectar_gsheet():
    creds_dict = st.secrets["gcp_service_account"]

    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )

    client = gspread.authorize(creds)
    return client.open_by_url(PLANILHA_URL).sheet1


# =========================
# DATA
# =========================
def preparar_datas(data_str):
    dt = datetime.strptime(data_str, "%d/%m/%Y")

    return {
        "yyyy": dt.strftime("%Y"),
        "mm": dt.strftime("%m"),
        "dd": dt.strftime("%d"),
        "yyyymmdd": dt.strftime("%Y%m%d"),
        "iso_exec": dt.strftime("%Y-%m-%dT06:00:00.000Z")
    }


# =========================
# URLS
# =========================
def montar_urls(d):
    return {
        "executivo_html": f"https://www.jornalminasgerais.mg.gov.br/edicao-do-dia?dados=%7B%22dataPublicacaoSelecionada%22:%22{d['iso_exec']}%22%7D",
        "legislativo": f"https://diariolegislativo.almg.gov.br/{d['yyyy']}/L{d['yyyymmdd']}.pdf",
    }


# =========================
# DOWNLOAD
# =========================
def baixar(url):
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content


# =========================
# EXECUTIVO (API CORRETA)
# =========================
def baixar_pdf_jornal_mg_por_link(url_pagina: str) -> bytes:
    try:
        match = re.search(r'dados=([^&]+)', url_pagina)
        if not match:
            raise Exception("Parâmetro dados não encontrado")

        dados_codificados = match.group(1)
        json_str = requests.utils.unquote(dados_codificados)
        dados = json.loads(json_str)

        data_iso = dados["dataPublicacaoSelecionada"]
        data = data_iso.split("T")[0]

        api_url = f"https://www.jornalminasgerais.mg.gov.br/api/v1/Jornal/ObterEdicaoPorDataPublicacao?dataPublicacao={data}"

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.jornalminasgerais.mg.gov.br/"
        }

        r = requests.get(api_url, headers=headers, timeout=60)
        r.raise_for_status()

        dados_api = r.json()

        base64_pdf = dados_api["dados"]["arquivoCadernoPrincipal"]["arquivo"]
        pdf_bytes = base64.b64decode(base64_pdf)

        return pdf_bytes

    except Exception as e:
        raise Exception(f"Erro ao obter PDF do Executivo: {e}")


# =========================
# =========================
# 🔴 CLASSES (SEM ALTERAÇÃO)
# =========================
# =========================

# 👉 COLE AQUI EXATAMENTE SUAS CLASSES:
# - LegislativeProcessor
# - AdministrativeProcessor
# - ExecutiveProcessor
# (não vou repetir aqui porque já estão corretas e enormes)

# =========================
# STREAMLIT
# =========================
st.title("📄 Diário MG → Automação")

data = st.text_input("Data (DD/MM/AAAA)", "17/03/2026")

if st.button("Processar"):

    d = preparar_datas(data)
    urls = montar_urls(d)

    st.write("🔎 Processando...")

    # ================= EXECUTIVO =================
    try:
        pdf_exec = baixar_pdf_jornal_mg_por_link(urls["executivo_html"])
        exec_proc = ExecutiveProcessor(pdf_exec)
        df_exec = exec_proc.process_pdf()
        st.success(f"Executivo OK ({len(df_exec)} registros)")
    except Exception as e:
        st.error(f"Erro Executivo: {e}")
        df_exec = pd.DataFrame()

    # ================= LEGISLATIVO =================
    try:
        pdf_leg = baixar(urls["legislativo"])
        leg_proc = LegislativeProcessor(pdf_leg)
        df_leg = leg_proc.process_all()["Normas"]
        st.success(f"Legislativo OK ({len(df_leg)} registros)")
    except Exception as e:
        st.error(f"Erro Legislativo: {e}")
        df_leg = pd.DataFrame()

    # ================= GOOGLE SHEETS =================
    try:
        sheet = conectar_gsheet()

        if not df_exec.empty:
            data_out = [df_exec.columns.tolist()] + df_exec.values.tolist()
            sheet.update("A1", data_out)

        st.success("Planilha atualizada 🚀")

    except Exception as e:
        st.error(f"Erro Google Sheets: {e}")
