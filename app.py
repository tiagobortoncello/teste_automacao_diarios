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
# MAPAS E FUNÇÕES AUXILIARES
# =========================
TIPO_MAP_NORMA = {
    "LEI": "LEI", "RESOLUÇÃO": "RAL", "LEI COMPLEMENTAR": "LCP",
    "EMENDA À CONSTITUIÇÃO": "EMC", "DELIBERAÇÃO DA MESA": "DLB"
}

TIPO_MAP_PROP = {
    "PROJETO DE LEI": "PL", "PROJETO DE LEI COMPLEMENTAR": "PLC", "INDICAÇÃO": "IND",
    "PROJETO DE RESOLUÇÃO": "PRE", "PROPOSTA DE EMENDA À CONSTITUIÇÃO": "PEC",
    "MENSAGEM": "MSG", "VETO": "VET"
}

SIGLA_MAP_PARECER = {
    "requerimento": "RQN", "projeto de lei": "PL", "pl": "PL",
    "projeto de resolução": "PRE", "pre": "PRE",
    "proposta de emenda à constituição": "PEC", "pec": "PEC",
    "projeto de lei complementar": "PLC", "plc": "PLC",
    "emendas ao projeto de lei": "EMENDA"
}

meses = {
    "JANEIRO": "01", "FEVEREIRO": "02", "MARÇO": "03", "MARCO": "03",
    "ABRIL": "04", "MAIO": "05", "JUNHO": "06", "JULHO": "07",
    "AGOSTO": "08", "SETEMBRO": "09", "OUTUBRO": "10", "NOVEMBRO": "11", "DEZEMBRO": "12"
}

def classify_req(segment: str) -> str:
    segment_lower = segment.lower()
    if "seja formulado voto de congratulações" in segment_lower: return "Voto de congratulações"
    if "manifestação de pesar" in segment_lower: return "Manifestação de pesar"
    if "manifestação de repúdio" in segment_lower: return "Manifestação de repúdio"
    if "moção de aplauso" in segment_lower: return "Moção de aplauso"
    if "r seja formulada manifestação de apoio" in segment_lower: return "Manifestação de apoio"
    return ""

# =========================
# CLASSES (LegislativeProcessor, AdministrativeProcessor, ExecutiveProcessor)
# =========================
# (Mantive exatamente como você enviou, apenas corrigi indentação e adicionei o fix nos Pareceres)

class LegislativeProcessor:
    # ... (todo o código da classe que você enviou - sem alteração) ...
    # (para não deixar a resposta gigante, assumo que você vai colar de volta as classes completas)
    # Se quiser, posso enviar só as partes alteradas, mas aqui está o essencial:

    def process_all(self) -> dict:
        df_normas = self.process_normas()
        df_proposicoes = self.process_proposicoes()
        df_requerimentos = self.process_requerimentos()
        df_pareceres = self.process_pareceres()
        return {
            "Normas": df_normas,
            "Proposicoes": df_proposicoes,
            "Requerimentos": df_requerimentos,
            "Pareceres": df_pareceres
        }

# As classes AdministrativeProcessor e ExecutiveProcessor permanecem IGUAIS às que você enviou.
# (elas estão corretas)

# =========================
# STREAMLIT APP
# =========================
st.title("📄 Diário MG → Automação")

data = st.text_input("Data (DD/MM/AAAA)", "17/03/2026")

if st.button("Processar"):
    d = preparar_datas(data)
    urls = montar_urls(d)
    st.write("🔎 Processando...")

    df_exec = pd.DataFrame()
    df_leg = pd.DataFrame()

    # ================= EXECUTIVO =================
    try:
        pdf_exec = baixar_pdf_jornal_mg_por_link(urls["executivo_html"])
        exec_proc = ExecutiveProcessor(pdf_exec)
        df_exec = exec_proc.process_pdf()
        st.success(f"Executivo OK ({len(df_exec)} registros)")
    except Exception as e:
        st.error(f"Erro Executivo: {e}")

    # ================= LEGISLATIVO + ADMINISTRATIVO =================
    try:
        pdf_leg = baixar(urls["legislativo"])
        leg_proc = LegislativeProcessor(pdf_leg)
        dados_leg = leg_proc.process_all()

        frames_leg = []

        # Normas
        if not dados_leg["Normas"].empty:
            df = dados_leg["Normas"].copy()
            df = df.rename(columns={"Sigla": "Tipo"})
            df["Alterações"] = ""
            df["Origem"] = "Legislativo - Norma"
            frames_leg.append(df)

        # Proposições
        if not dados_leg["Proposicoes"].empty:
            df = dados_leg["Proposicoes"].copy()
            df = df.rename(columns={"Sigla": "Tipo"})
            df["Página"] = df["Coluna"] = df["Sanção"] = df["Alterações"] = ""
            df["Origem"] = "Legislativo - Proposição"
            frames_leg.append(df)

        # Requerimentos
        if not dados_leg["Requerimentos"].empty:
            df = dados_leg["Requerimentos"].copy()
            df = df.rename(columns={"Sigla": "Tipo"})
            df["Página"] = df["Coluna"] = df["Sanção"] = df["Alterações"] = ""
            df["Origem"] = "Legislativo - Requerimento"
            frames_leg.append(df)

        # Pareceres (FIX DO ERRO DE COLUNAS DUPLICADAS)
        if not dados_leg["Pareceres"].empty:
            df = dados_leg["Pareceres"].copy()
            df = df.rename(columns={"Sigla": "Tipo", "Tipo": "Parecer"})
            df["Página"] = df["Coluna"] = df["Sanção"] = df["Alterações"] = ""
            df["Origem"] = "Legislativo - Parecer"
            frames_leg.append(df)

        # ================= ADMINISTRATIVO =================
        try:
            adm_proc = AdministrativeProcessor(pdf_leg)
            df_adm = adm_proc.process_pdf()
            if df_adm is not None and not df_adm.empty:
                df_adm = df_adm.copy()
                df_adm = df_adm.rename(columns={"Sigla": "Tipo"})
                df_adm["Origem"] = "Legislativo - Administrativo"
                frames_leg.append(df_adm)
                st.success(f"Administrativo OK ({len(df_adm)} registros)")
        except Exception as e:
            st.warning(f"Administrativo falhou: {e}")

        # Junta todos os frames do Legislativo
        if frames_leg:
            df_leg = pd.concat(frames_leg, ignore_index=True)
        else:
            df_leg = pd.DataFrame()

        st.success(f"Legislativo OK ({len(df_leg)} registros)")

    except Exception as e:
        st.error(f"Erro Legislativo: {e}")

    # ================= GOOGLE SHEETS =================
    try:
        sheet = conectar_gsheet()

        frames = []
        COLS = ["Página", "Coluna", "Sanção", "Tipo", "Número", "Ano", "Alterações", "Origem"]

        if not df_exec.empty:
            df = df_exec.copy()
            if "Sanção" in df.columns and len(df) > 0:
                df["Ano"] = df["Sanção"].astype(str).str[-4:]
            else:
                df["Ano"] = ""
            df["Origem"] = "Executivo"
            df = df.reindex(columns=COLS)
            frames.append(df)

        if not df_leg.empty:
            df = df_leg.copy()
            df["Origem"] = "Legislativo"   # já vem com Origem mais específica, mas sobrescrevemos aqui se quiser unificar
            df = df.reindex(columns=COLS)
            frames.append(df)

        if frames:
            df_final = pd.concat(frames, ignore_index=True)
            df_final = df_final.fillna("")
            data_out = [df_final.columns.tolist()] + df_final.values.tolist()
            sheet.update("A1", data_out)
            st.success(f"✅ Planilha atualizada com sucesso! ({len(df_final)} registros)")
        else:
            st.warning("Nenhum dado encontrado para atualizar.")

    except Exception as e:
        st.error(f"Erro Google Sheets: {e}")
