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
TIPO_MAP_NORMA = {
    "LEI": "LEI",
    "RESOLUÇÃO": "RAL",
    "LEI COMPLEMENTAR": "LCP",
    "EMENDA À CONSTITUIÇÃO": "EMC",
    "DELIBERAÇÃO DA MESA": "DLB"
}

TIPO_MAP_PROP = {
    "PROJETO DE LEI": "PL",
    "PROJETO DE LEI COMPLEMENTAR": "PLC",
    "INDICAÇÃO": "IND",
    "PROJETO DE RESOLUÇÃO": "PRE",
    "PROPOSTA DE EMENDA À CONSTITUIÇÃO": "PEC",
    "MENSAGEM": "MSG",
    "VETO": "VET"
}

SIGLA_MAP_PARECER = {
    "requerimento": "RQN",
    "projeto de lei": "PL",
    "pl": "PL",
    "projeto de resolução": "PRE",
    "pre": "PRE",
    "proposta de emenda à constituição": "PEC",
    "pec": "PEC",
    "projeto de lei complementar": "PLC",
    "plc": "PLC",
    "emendas ao projeto de lei": "EMENDA"
}

meses = {
    "JANEIRO": "01", "FEVEREIRO": "02", "MARÇO": "03", "MARCO": "03",
    "ABRIL": "04", "MAIO": "05", "JUNHO": "06", "JULHO": "07",
    "AGOSTO": "08", "SETEMBRO": "09", "OUTUBRO": "10", "NOVEMBRO": "11", "DEZEMBRO": "12"
}

# --- Funções Utilitárias para Extrator de Diários Oficiais ---
def classify_req(segment: str) -> str:
    segment_lower = segment.lower()
    if "seja formulado voto de congratulações" in segment_lower:
        return "Voto de congratulações"
    if "manifestação de pesar" in segment_lower:
        return "Manifestação de pesar"
    if "manifestação de repúdio" in segment_lower:
        return "Manifestação de repúdio"
    if "moção de aplauso" in segment_lower:
        return "Moção de aplauso"
    if "r seja formulada manifestação de apoio" in segment_lower:
        return "Manifestação de apoio"
    return ""

# --- Classes de Processamento para Extrator de Diários Oficiais ---
class LegislativeProcessor:
    def __init__(self, pdf_bytes: bytes):
        self.pdf_bytes = pdf_bytes

        reader = pypdf.PdfReader(io.BytesIO(self.pdf_bytes))

        # Extrai por página e preserva quebras de linha (IMPORTANTE p/ regex com MULTILINE e ^)
        page_texts = []
        for page in reader.pages:
            pt = page.extract_text() or ""
            # Normaliza apenas espaços/tabs, sem mexer em \n
            pt = re.sub(r"[ \t]+", " ", pt)
            page_texts.append(pt)

        # Monta texto global com offsets por página
        self._offsets = []  # (start, end, page_number)
        parts = []
        cursor = 0

        for idx, pt in enumerate(page_texts, start=1):
            chunk = pt + "\n"  # separador estável entre páginas
            start = cursor
            end = cursor + len(chunk)
            self._offsets.append((start, end, idx))
            parts.append(chunk)
            cursor = end

        self.text = "".join(parts)

    def _pagina_from_pos(self, pos: int) -> str:
        for start, end, pnum in self._offsets:
            if start <= pos < end:
                return str(pnum)
        return ""

    def process_normas(self) -> pd.DataFrame:
        pattern = re.compile(
            r"^(LEI COMPLEMENTAR|LEI|RESOLUÇÃO|EMENDA À CONSTITUIÇÃO|DELIBERAÇÃO DA MESA) Nº (\d{1,5}(?:\.\d{0,3})?)(?:/(\d{4}))?(?:, DE .+ DE (\d{4}))?$",
            re.MULTILINE
        )

        data_na_epigrafe_regex = re.compile(
            r"\bDE\s+(\d{1,2})\s+DE\s+([A-ZÇÃÁÉÍÓÔÚ]+)\s+DE\s+(\d{4})\b",
            re.IGNORECASE
        )

        meses_leg = {
            "JANEIRO": "01", "FEVEREIRO": "02", "MARÇO": "03", "MARCO": "03",
            "ABRIL": "04", "MAIO": "05", "JUNHO": "06", "JULHO": "07",
            "AGOSTO": "08", "SETEMBRO": "09", "OUTUBRO": "10", "NOVEMBRO": "11", "DEZEMBRO": "12"
        }

        normas = []
        for match in pattern.finditer(self.text):
            tipo_extenso = match.group(1)
            numero_raw = match.group(2).replace(".", "")
            ano = match.group(3) if match.group(3) else match.group(4)
            if not ano:
                continue

            pagina = self._pagina_from_pos(match.start())
            coluna = 1  # como combinado

            sancao = ""
            linha_epigrafe = match.group(0) or ""
            dm = data_na_epigrafe_regex.search(linha_epigrafe)
            if dm:
                dia = (dm.group(1) or "").zfill(2)
                mes_nome = (dm.group(2) or "").upper().strip()
                mes = meses_leg.get(mes_nome, "")
                ano_data = (dm.group(3) or "").strip()
                if mes:
                    sancao = f"{dia}/{mes}/{ano_data}"

            sigla = TIPO_MAP_NORMA[tipo_extenso]
            normas.append([pagina, coluna, sancao, sigla, numero_raw, ano])

        return pd.DataFrame(normas, columns=['Página', 'Coluna', 'Sanção', 'Sigla', 'Número', 'Ano'])

    def process_proposicoes(self) -> pd.DataFrame:
        pattern_prop = re.compile(
            r"^\s*(?:- )?\s*(PROJETO DE LEI COMPLEMENTAR|PROJETO DE LEI|INDICAÇÃO|PROJETO DE RESOLUÇÃO|PROPOSTA DE EMENDA À CONSTITUIÇÃO|MENSAGEM|VETO) Nº (\d{1,4}\.?\d{0,3}/\d{4})",
            re.MULTILINE
        )
        pattern_utilidade = re.compile(r"Declara de utilidade pública", re.IGNORECASE | re.DOTALL)
        ignore_redacao_final = re.compile(r"opinamos por se dar à proposição a seguinte redação final", re.IGNORECASE)
        ignore_publicada_antes = re.compile(r"foi publicad[ao] na edição anterior\.", re.IGNORECASE)
        ignore_em_epigrafe = re.compile(r"Na publicação da matéria em epígrafe", re.IGNORECASE)

        proposicoes = []
        for match in pattern_prop.finditer(self.text):
            start_idx = match.start()
            end_idx = match.end()
            contexto_antes = self.text[max(0, start_idx - 200):start_idx]
            contexto_depois = self.text[end_idx:end_idx + 250]

            if ignore_em_epigrafe.search(contexto_depois):
                continue
            if ignore_redacao_final.search(contexto_antes) or ignore_publicada_antes.search(contexto_depois):
                continue
            subseq_text = self.text[end_idx:end_idx + 250]
            if "(Redação do Vencido)" in subseq_text:
                continue

            tipo_extenso = match.group(1)
            numero_ano = match.group(2).replace(".", "")
            numero, ano = numero_ano.split("/")
            sigla = TIPO_MAP_PROP[tipo_extenso]
            categoria = "UP" if pattern_utilidade.search(subseq_text) else ""
            proposicoes.append([sigla, numero, ano, categoria])

        return pd.DataFrame(
            proposicoes,
            columns=['Sigla', 'Número', 'Ano', 'Categoria']
        )

    def process_requerimentos(self) -> pd.DataFrame:
        # === SEU CÓDIGO ORIGINAL, SEM MUDAR REGRAS ===
        requerimentos = []

        ignore_officio_pattern = re.compile(
            r"Ofício[\s\S]{0,200}?Requerimento\s*n[ºo]?\s*(\d{1,5}(?:\.\d{0,3})?)/(\d{4})",
            re.IGNORECASE
        )

        ignore_anexese_pattern = re.compile(
            r"Anexe-se\s+ao\s+Requerimento\s*n[ºo]?\s*(\d{1,5}(?:\.\d{0,3})?)/(\d{4})",
            re.IGNORECASE
        )

        ignore_relativas_pattern = re.compile(
            r"(?:relativa[s]?|referente[s]?|informações\s+relativas\s+ao)"
            r"[\s\S]{0,80}?Requerimento\s*n[ºo]?\s*(\d{1,5}(?:\.\d{0,3})?)/(\d{4})",
            re.IGNORECASE
        )

        reqs_to_ignore = set()

        for match in ignore_officio_pattern.finditer(self.text):
            num_part = match.group(1).replace('.', '')
            ano = match.group(2)
            reqs_to_ignore.add(f"{num_part}/{ano}")

        for match in ignore_anexese_pattern.finditer(self.text):
            num_part = match.group(1).replace('.', '')
            ano = match.group(2)
            reqs_to_ignore.add(f"{num_part}/{ano}")

        for match in ignore_relativas_pattern.finditer(self.text):
            num_part = match.group(1).replace('.', '')
            ano = match.group(2)
            reqs_to_ignore.add(f"{num_part}/{ano}")

        ignore_pattern = re.compile(
            r"Ofício nº .*?,.*?relativas ao Requerimento\s*nº (\d{1,4}\.?\d{0,3}/\d{4})",
            re.IGNORECASE | re.DOTALL
        )
        aprovado_pattern = re.compile(
            r"(da Comissão.*?, informando que, na.*?foi aprovado o Requerimento\s*nº (\d{1,5}(?:\.\d{0,3})?)/(\d{4}))",
            re.IGNORECASE | re.DOTALL
        )

        for match in ignore_pattern.finditer(self.text):
            numero_ano = match.group(1).replace(".", "")
            reqs_to_ignore.add(numero_ano)

        for match in aprovado_pattern.finditer(self.text):
            num_part = match.group(2).replace('.', '')
            ano = match.group(3)
            numero_ano = f"{num_part}/{ano}"
            reqs_to_ignore.add(numero_ano)

        req_recebimento_pattern = re.compile(
            r"RECEBIMENTO DE PROPOSIÇÃO[\s\S]*?REQUERIMENTO Nº (\d{1,5}(?:\.\d{0,3})?)/(\d{4})",
            re.IGNORECASE | re.DOTALL
        )
        for match in req_recebimento_pattern.finditer(self.text):
            num_part = match.group(1).replace('.', '')
            ano = match.group(2)
            numero_ano = f"{num_part}/{ano}"
            if numero_ano not in reqs_to_ignore:
                requerimentos.append(["RQN", num_part, ano, "", "", "Recebido"])

        rqc_pattern_aprovado = re.compile(
            r"É\s+recebido\s+pela\s+presidência,\s+submetido\s+a\s+votação\s+e\s+aprovado\s+o\s+Requerimento(?:s)?(?: nº| Nº| n\u00ba| n\u00b0)?\s*(\d{1,5}(?:\.\d{0,3})?)/\s*(\d{4})",
            re.IGNORECASE
        )
        for match in rqc_pattern_aprovado.finditer(self.text):
            num_part = match.group(1).replace('.', '')
            ano = match.group(2)
            numero_ano = f"{num_part}/{ano}"
            if numero_ano not in reqs_to_ignore:
                requerimentos.append(["RQC", num_part, ano, "", "", "Aprovado"])

        rqc_recebido_apreciacao_pattern = re.compile(
            r"É recebido pela\s+presidência, para posterior apreciação, o Requerimento(?: nº| Nº)?\s*(\d{1,5}(?:\.\d{0,3})?)/(\d{4})",
            re.IGNORECASE | re.DOTALL
        )
        for match in rqc_recebido_apreciacao_pattern.finditer(self.text):
            num_part = match.group(1).replace('.', '')
            ano = match.group(2)
            numero_ano = f"{num_part}/{ano}"
            if numero_ano not in reqs_to_ignore:
                requerimentos.append(["RQC", num_part, ano, "", "", "Recebido para apreciação"])

        rqc_prejudicado_pattern = re.compile(
            r"é\s+prejudicado\s+o\s+Requerimento(?: nº| Nº| n\u00ba| n\u00b0)?\s*(\d{1,5}(?:\.\d{0,3})?)/\s*(\d{4})",
            re.IGNORECASE | re.DOTALL
        )
        for match in rqc_prejudicado_pattern.finditer(self.text):
            num_part = match.group(1).replace('.', '')
            ano = match.group(2)
            numero_ano = f"{num_part}/{ano}"
            if numero_ano not in reqs_to_ignore:
                requerimentos.append(["RQC", num_part, ano, "", "", "Prejudicado"])

        rqc_rejeitado_pattern = re.compile(
            r"É\s+recebido\s+pela\s+presidência,\s+submetido\s+a\s+votação\s+e\s+rejeitado\s+o\s+Requerimento(?:s)?(?: nº| Nº| n\u00ba| n\u00b0)?\s*(\d{1,5}(?:\.\d{0,3})?)/\s*(\d{4})",
            re.IGNORECASE | re.DOTALL
        )
        for match in rqc_rejeitado_pattern.finditer(self.text):
            num_part = match.group(1).replace('.', '')
            ano = match.group(2)
            numero_ano = f"{num_part}/{ano}"
            if numero_ano not in reqs_to_ignore:
                requerimentos.append(["RQC", num_part, ano, "", "", "Rejeitado"])

        rqn_pattern = re.compile(r"^(?:\s*)(Nº)\s+(\d{2}\.?\d{3}/\d{4})\s*,\s*(do|da)", re.MULTILINE)
        rqc_old_pattern = re.compile(r"^(?:\s*)(nº)\s+(\d{2}\.?\d{3}/\d{4})\s*,\s*(do|da)", re.MULTILINE)
        for pattern, sigla_prefix in [(rqn_pattern, "RQN"), (rqc_old_pattern, "RQC")]:
            for match in pattern.finditer(self.text):
                start_idx = match.start()
                next_match = re.search(
                    r"^(?:\s*)(Nº|nº)\s+(\d{2}\.?\d{3}/\d{4})",
                    self.text[start_idx + 1:], flags=re.MULTILINE
                )
                end_idx = (next_match.start() + start_idx + 1) if next_match else len(self.text)
                block = self.text[start_idx:end_idx].strip()
                nums_in_block = re.findall(r'\d{2}\.?\d{3}/\d{4}', block)
                if not nums_in_block:
                    continue
                num_part, ano = nums_in_block[0].replace(".", "").split("/")
                numero_ano = f"{num_part}/{ano}"
                if numero_ano not in reqs_to_ignore:
                    classif = classify_req(block)
                    requerimentos.append([sigla_prefix, num_part, ano, "", "", classif])

        nao_recebidas_header_pattern = re.compile(r"PROPOSIÇÕES\s*NÃO\s*RECEBIDAS", re.IGNORECASE)
        header_match = nao_recebidas_header_pattern.search(self.text)
        if header_match:
            start_idx = header_match.end()
            next_section_pattern = re.compile(r"^\s*(\*?)\s*.*\s*(\*?)\s*$", re.MULTILINE)
            next_section_match = next_section_pattern.search(self.text, start_idx)
            end_idx = next_section_match.start() if next_section_match else len(self.text)
            nao_recebidos_block = self.text[start_idx:end_idx]
            rqn_nao_recebido_pattern = re.compile(r"REQUERIMENTO Nº (\d{2}\.?\d{3}/\d{4})", re.IGNORECASE)
            for match in rqn_nao_recebido_pattern.finditer(nao_recebidos_block):
                numero_ano = match.group(1).replace(".", "")
                num_part, ano = numero_ano.split("/")
                if numero_ano not in reqs_to_ignore:
                    requerimentos.append(["RQN", num_part, ano, "", "", "NÃO RECEBIDO"])

        unique_reqs = []
        seen = set()
        for r in requerimentos:
            key = (r[0], r[1], r[2])
            if key not in seen:
                seen.add(key)
                unique_reqs.append(r)

        return pd.DataFrame(unique_reqs, columns=['Sigla', 'Número', 'Ano', 'Coluna4', 'Coluna5', 'Classificação'])

    def process_pareceres(self) -> pd.DataFrame:
        # === SEU CÓDIGO ORIGINAL (igual ao que você enviou), sem mudar regras ===
        found_projects = {}
        pareceres_start_pattern = re.compile(r"TRAMITAÇÃO DE PROPOSIÇÕES")
        votacao_pattern = re.compile(
            r"(Votação do Requerimento[\s\S]*?)(?=Votação do Requerimento|Diário do Legislativo|Projetos de Lei Complementar|Diário do Legislativo - Poder Legislativo|$)",
            re.IGNORECASE
        )
        pareceres_start = pareceres_start_pattern.search(self.text)
        if not pareceres_start:
            return pd.DataFrame(columns=['Sigla', 'Número', 'Ano', 'Tipo'])

        pareceres_text = self.text[pareceres_start.end():]
        clean_text = pareceres_text
        for match in votacao_pattern.finditer(pareceres_text):
            clean_text = clean_text.replace(match.group(0), "")

        emenda_projeto_lei_pattern = re.compile(
            r"EMENDAS AO PROJETO DE LEI Nº (\d{1,4}\.?\d{0,3})/(\d{4})",
            re.IGNORECASE | re.DOTALL
        )
        for match in emenda_projeto_lei_pattern.finditer(clean_text):
            numero_raw = match.group(1).replace('.', '')
            ano = match.group(2)
            project_key = ("PL", numero_raw, ano)
            if project_key not in found_projects:
                found_projects[project_key] = set()
            found_projects[project_key].add("EMENDA")

        emenda_completa_pattern = re.compile(
            r"EMENDA Nº (\d+)\s+AO\s+(?:SUBSTITUTIVO Nº \d+\s+AO\s+)?PROJETO DE LEI(?: COMPLEMENTAR)? Nº (\d{1,4}\.?\d{0,3})/(\d{4})",
            re.IGNORECASE
        )
        emenda_pattern = re.compile(r"^(?:\s*)EMENDA Nº (\d+)\s*", re.MULTILINE)
        substitutivo_pattern = re.compile(r"^(?:\s*)SUBSTITUTIVO Nº (\d+)\s*", re.MULTILINE)
        project_pattern = re.compile(
            r"Conclusão\s*([\s\S]*?)(Projeto de Lei|PL|Projeto de Resolução|PRE|Proposta de Emenda à Constituição|PEC|Projeto de Lei Complementar|PLC|Requerimento)\s+(?:nº|Nº)?\s*(\d{1,4}(?:\.\d{1,3})?)\s*/\s*(\d{4})",
            re.IGNORECASE | re.DOTALL
        )

        for match in emenda_completa_pattern.finditer(clean_text):
            numero = match.group(2).replace(".", "")
            ano = match.group(3)
            sigla = "PLC" if "COMPLEMENTAR" in match.group(0).upper() else "PL"
            project_key = (sigla, numero, ano)
            if project_key not in found_projects:
                found_projects[project_key] = set()
            found_projects[project_key].add("EMENDA")

        all_matches = sorted(
            list(emenda_pattern.finditer(clean_text)) + list(substitutivo_pattern.finditer(clean_text)),
            key=lambda x: x.start()
        )

        for title_match in all_matches:
            text_before_title = clean_text[:title_match.start()]
            last_project_match = None
            for match in project_pattern.finditer(text_before_title):
                last_project_match = match

            if last_project_match:
                sigla_raw = last_project_match.group(2)
                sigla = SIGLA_MAP_PARECER.get(sigla_raw.lower(), sigla_raw.upper())
                numero = last_project_match.group(3).replace(".", "")
                ano = last_project_match.group(4)
                project_key = (sigla, numero, ano)
                item_type = "EMENDA" if "EMENDA" in title_match.group(0).upper() else "SUBSTITUTIVO"
                if project_key not in found_projects:
                    found_projects[project_key] = set()
                found_projects[project_key].add(item_type)

        emenda_projeto_lei_pattern = re.compile(r"EMENDAS AO PROJETO DE LEI Nº (\d{1,4}\.?\d{0,3})/(\d{4})", re.IGNORECASE)
        for match in emenda_projeto_lei_pattern.finditer(clean_text):
            numero_raw = match.group(1).replace('.', '')
            ano = match.group(2)
            project_key = ("PL", numero_raw, ano)
            if project_key not in found_projects:
                found_projects[project_key] = set()
            found_projects[project_key].add("EMENDA")

        pareceres = []
        for (sigla, numero, ano), types in found_projects.items():
            type_str = "SUB/EMENDA" if len(types) > 1 else list(types)[0]
            pareceres.append([sigla, numero, ano, type_str])

        return pd.DataFrame(pareceres, columns=['Sigla', 'Número', 'Ano', 'Tipo'])

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

class AdministrativeProcessor:
    def __init__(self, pdf_bytes: bytes):
        self.pdf_bytes = pdf_bytes

        # Meses para converter "15 de dezembro de 2025" -> 15/12/2025
        self.meses = {
            "janeiro": "01", "fevereiro": "02", "março": "03", "marco": "03",
            "abril": "04", "maio": "05", "junho": "06", "julho": "07",
            "agosto": "08", "setembro": "09", "outubro": "10", "novembro": "11", "dezembro": "12"
        }

        # --- (1) Norma publicada (inclui DGE, PSEC/DGE, PRES/DGE, PRES/PSEC) ---
        self.norma_publicada_regex = re.compile(
            r'^(DELIBERAÇÃO DA MESA|'
            r'PORTARIA\s+(?:DGE|PSEC\s*/\s*DGE|PRES\s*/\s*DGE|PRES\s*/\s*PSEC)|'
            r'ORDEM DE SERVIÇO PRES/PSEC)\s+N[º°]\s+([\d\.]+)\s*/\s*(\d{4})\s*$',
            re.IGNORECASE | re.MULTILINE
        )

        # --- (2) Caput gatilho (lista longa) ---
        self.revogacoes_caput_regex = re.compile(
            r'Ficam\s+revogados\s+os\s+seguintes\s+atos\s+normativos,'
            r'\s+sem\s+preju[ií]zo\s+dos\s+efeitos\s+por\s+eles\s+produzidos\s*:',
            re.IGNORECASE
        )

        # --- outros gatilhos ---
        self.revogacao_simples_regex = re.compile(r'\bFic(?:a|am)\s+revogad(?:a|o|as|os)\b', re.IGNORECASE)
        self.sem_efeito_regex = re.compile(r'\bFic(?:a|am)\s+sem\s+efeito\b|\bTorn(?:a|am)\s+sem\s+efeito\b', re.IGNORECASE)
        self.prorrogacao_regex = re.compile(r'\bFic(?:a|am)\s+prorrogad(?:a|o|as|os)\b', re.IGNORECASE)
        self.redacao_regex = re.compile(
            r'\bpassa\s+a\s+vigorar\b|\bpassam\s+a\s+vigorar\b|\bpassa\s+a\s+vigorar\s+com\s+a\s+seguinte\s+reda[cç][aã]o\b',
            re.IGNORECASE
        )

        dash = r'[–—-]'

        # --- (3) Terminadores estruturais (para lista) ---
        self.fim_lista_revogacoes_regex = re.compile(
            rf'\bArt\.\s*\d+º?\s*{dash}\s*|\bArtigo\s+\d+º?\s*{dash}\s*',
            re.IGNORECASE
        )

        # --- (4) Norma alvo alterada (inclui as variações que vocês já validaram) ---
        self.norma_alterada_regex = re.compile(
            rf'\b('
            rf'DELIBERAÇÃO\s+DA\s+MESA|'

            rf'PORTARIA'
            rf'(?:'
                rf'\s+DA\s+PRESID[ÊE]NCIA\s+E\s+DA\s+DIRETORIA-GERAL'
                rf'|'
                rf'\s+DA\s+1ª-SECRETARIA\s*{dash}\s*PSEC\s*{dash}\s*E\s+DA\s+DIRETORIA-GERAL\s*{dash}\s*DGE\s*{dash}'
                rf'|'
                rf'\s+DA\s+DIRETORIA-GERAL(?:\s*{dash}\s*DGE\s*{dash})?'
                rf'|'
                rf'\s*PSEC\s*/\s*DGE'
                rf'|'
                rf'\s*PRES\s*/\s*DGE'
                rf'|'
                rf'\s*PRES\s*/\s*PSEC'
                rf'|'
                rf'\s*DGE'
            rf')?'

            rf'|'

            rf'ORDEM\s+DE\s+SERVI[ÇC]O\s+PRES/PSEC|'
            rf'ORDEM\s+DE\s+SERVI[ÇC]O\s+DA\s+PRESID[ÊE]NCIA\s+E\s+DA\s+1ª-SECRETARIA|'
            rf'ORDEM\s+DE\s+SERVI[ÇC]O'
            rf')\s*N[º°]\s*([\d\.]+)'
            rf'(?:\s*/\s*(\d{{4}}))?'
            rf'(?:\s*,\s*de\s*[^;\.]*?(\d{{4}}))?',
            re.IGNORECASE
        )

        # --- (5) Fechos (sanção): 2 padrões ---
        self.fecho_palacio_regex = re.compile(
            r'Pal[aá]cio\s+da\s+Inconfid[eê]ncia\s*,\s*'
            r'(\d{1,2})\s+de\s+([A-Za-zçÇãÃáÁéÉíÍóÓôÔúÚ]+)\s+de\s+(\d{4})',
            re.IGNORECASE
        )
        self.fecho_sala_mesa_regex = re.compile(
            r'Sala\s+de\s+Reuni[õo]es\s+da\s+Mesa\s+da\s+Assembleia\s+Legislativa\s*,\s*'
            r'(\d{1,2})\s+de\s+([A-Za-zçÇãÃáÁéÉíÍóÓôÔúÚ]+)\s+de\s+(\d{4})',
            re.IGNORECASE
        )

        # --- (6) DCS ---
        self.regex_dcs = re.compile(r'DECIS[ÃA]O DA 1ª-SECRETARIA', re.IGNORECASE)

    def _formatar_data_fecho(self, bloco: str) -> str:
        bloco = bloco or ""

        m = self.fecho_palacio_regex.search(bloco)
        if not m:
            m = self.fecho_sala_mesa_regex.search(bloco)
        if not m:
            return ""

        dia = m.group(1).zfill(2)
        mes_nome = (m.group(2) or "").strip().lower()
        ano = (m.group(3) or "").strip()
        mes = self.meses.get(mes_nome, "")
        if not mes:
            return ""
        return f"{dia}/{mes}/{ano}"

    def _normalizar_sigla(self, tipo_txt_upper: str) -> str:
        t = (tipo_txt_upper or "").upper()
        if "DELIBERAÇÃO DA MESA" in t:
            return "DLB"
        if "PORTARIA" in t:
            return "PRT"
        if "ORDEM DE SERVI" in t:
            return "OSV"
        return t.strip()

    def _sigla_norma_publicada(self, tipo_raw: str) -> str:
        t = (tipo_raw or "").upper().strip()
        t = re.sub(r'\s+', ' ', t)
        t = re.sub(r'\s*/\s*', '/', t)
        return {
            "DELIBERAÇÃO DA MESA": "DLB",
            "PORTARIA DGE": "PRT",
            "PORTARIA PSEC/DGE": "PRT",
            "PORTARIA PRES/DGE": "PRT",
            "PORTARIA PRES/PSEC": "PRT",
            "ORDEM DE SERVIÇO PRES/PSEC": "OSV",
        }.get(t, "")

    def process_pdf(self):
        try:
            reader = pypdf.PdfReader(io.BytesIO(self.pdf_bytes))
        except Exception as e:
            st.error(f"Erro ao abrir o arquivo PDF: {e}")
            return None

        page_texts = []
        for p in reader.pages:
            page_texts.append(p.extract_text() or "")

        offsets = []
        full_text_parts = []
        cursor = 0
        for idx, pt in enumerate(page_texts, start=1):
            full_text_parts.append(pt + "\n")
            cursor_end = cursor + len(pt) + 1
            offsets.append((cursor, cursor_end, idx))
            cursor = cursor_end

        full_text = "".join(full_text_parts)
        full_text = re.sub(r"[ \t]+", " ", full_text)
        full_text = re.sub(r"\n+", "\n", full_text)

        def _pagina_from_pos(pos: int):
            for start, end, pnum in offsets:
                if start <= pos < end:
                    return pnum
            return ""

        normas = []
        for m in self.norma_publicada_regex.finditer(full_text):
            pos = m.start()
            pagina = _pagina_from_pos(pos)

            tipo_raw = m.group(1)
            numero = (m.group(2) or "").replace(".", "").replace(" ", "")
            ano = (m.group(3) or "").strip()

            sigla = self._sigla_norma_publicada(tipo_raw)
            if sigla:
                normas.append({
                    "pos": pos,
                    "end": m.end(),
                    "pagina": pagina,
                    "coluna": 1,
                    "sigla": sigla,
                    "numero": numero,
                    "ano": ano
                })

        resultados = []

        for i, n in enumerate(normas):
            start = n["end"]
            end = normas[i + 1]["pos"] if i + 1 < len(normas) else len(full_text)
            bloco = full_text[start:end]

            linha = {
                "Página": n["pagina"],
                "Coluna": n["coluna"],
                "Sanção": self._formatar_data_fecho(bloco),
                "Sigla": n["sigla"],
                "Número": n["numero"],
                "Ano": n["ano"],
                "Alterações": ""
            }
            resultados.append(linha)

            seen_alteracoes = set()

            def _add_alt(chave: str):
                nonlocal resultados
                if chave in seen_alteracoes:
                    return
                seen_alteracoes.add(chave)

                if linha["Alterações"] == "":
                    linha["Alterações"] = chave
                else:
                    resultados.append({
                        "Página": "",
                        "Coluna": "",
                        "Sanção": "",
                        "Sigla": "",
                        "Número": "",
                        "Ano": "",
                        "Alterações": chave
                    })

            def _extrair_alteracoes(seg: str):
                for alt in self.norma_alterada_regex.finditer(seg or ""):
                    tipo_alt_raw = (alt.group(1) or "").upper().strip()
                    num_alt = (alt.group(2) or "").replace(".", "").replace(" ", "")
                    ano_alt = alt.group(3) or alt.group(4) or ""
                    sigla_alt = self._normalizar_sigla(tipo_alt_raw)

                    if sigla_alt == linha["Sigla"] and num_alt == linha["Número"]:
                        if (not ano_alt) or (ano_alt == linha["Ano"]):
                            continue

                    chave = f"{sigla_alt} {num_alt}" + (f" {ano_alt}" if ano_alt else "")
                    _add_alt(chave)

            cap = self.revogacoes_caput_regex.search(bloco)
            if cap:
                after = bloco[cap.end():]
                fim = None
                m_art = self.fim_lista_revogacoes_regex.search(after)
                if m_art:
                    fim = m_art.start()
                segmento = after[:fim] if fim is not None else after
                _extrair_alteracoes(segmento)

            for gat in (self.revogacao_simples_regex, self.sem_efeito_regex, self.prorrogacao_regex):
                for gm in gat.finditer(bloco):
                    janela = bloco[gm.start(): gm.start() + 1200]
                    _extrair_alteracoes(janela)

            for gm in self.redacao_regex.finditer(bloco):
                start_j = max(0, gm.start() - 600)
                end_j = min(len(bloco), gm.end() + 1200)
                janela = bloco[start_j:end_j]
                _extrair_alteracoes(janela)

        if self.regex_dcs.search(full_text):
            resultados.append({
                "Página": "",
                "Coluna": 1,
                "Sanção": "",
                "Sigla": "DCS",
                "Número": "",
                "Ano": "",
                "Alterações": ""
            })

        return pd.DataFrame(
            resultados,
            columns=['Página', 'Coluna', 'Sanção', 'Sigla', 'Número', 'Ano', 'Alterações']
        )

    def to_csv(self):
        df = self.process_pdf()
        if df is None or df.empty:
            return None
        output_csv = io.StringIO()
        df.to_csv(output_csv, index=False, encoding="utf-8-sig")
        return output_csv.getvalue().encode('utf-8-sig')

class ExecutiveProcessor:
    def __init__(self, pdf_bytes: bytes):
        self.pdf_bytes = self._clean_pdf_bytes(pdf_bytes)

        self.mapa_tipos = {
            "LEI": "LEI",
            "LEI COMPLEMENTAR": "LCP",
            "DECRETO": "DEC",
            "DECRETO NE": "DNE"
        }

        self.norma_regex = re.compile(
    r'(?:^|\n|\r|\f)\s*(LEI\s+COMPLEMENTAR|LEI|DECRETO\s+NE|DECRETO)\s+N[º°]\s*([\d\s\.]+),?\s*DE\s+(.+?)(?:\n|$)',
    re.DOTALL
        )
        self.comandos_regex = re.compile(
            r'(Ficam\s+revogados|Fica\s+acrescentado|Ficam\s+alterados|passando\s+o\s+item|passa\s+a\s+vigorar|passam\s+a\s+vigorar)',
            re.IGNORECASE
        )
        self.norma_alterada_regex = re.compile(
            r'(LEI\s+COMPLEMENTAR|LEI|DECRETO\s+NE|DECRETO)\s+N[º°]?\s*([\d\s\./]+)(?:,\s*de\s*(.*?\d{4})?)?',
            re.IGNORECASE
        )

    def _clean_pdf_bytes(self, dirty_bytes: bytes) -> bytes:
        pdf_signature = b'%PDF-'
        try:
            start_index = dirty_bytes.index(pdf_signature)
            if start_index > 0:
                return dirty_bytes[start_index:]
            return dirty_bytes
        except ValueError:
            return dirty_bytes

    def find_relevant_pages(self) -> tuple:
        try:
            reader = pypdf.PdfReader(io.BytesIO(self.pdf_bytes))
            start_page_num, end_page_num = None, None
            for i, page in enumerate(reader.pages):
                text = page.extract_text() or ""
                if not text.strip():
                    continue
                if re.search(r'Leis\s*e\s*Decretos', text, re.IGNORECASE):
                    start_page_num = i
                if re.search(r'Atos\s*do\s*Governador', text, re.IGNORECASE):
                    end_page_num = i
            if start_page_num is None or end_page_num is None or start_page_num > end_page_num:
                st.warning("Não foi encontrado o trecho de 'Leis e Decretos' ou 'Atos do Governador' para delimitar a seção.")
                return None, None
            return start_page_num, end_page_num + 1
        except Exception as e:
            st.error(f"Erro ao buscar páginas relevantes com PyPDF: {e}")
            return None, None

    def process_pdf(self) -> pd.DataFrame:
        start_page_idx, end_page_idx = self.find_relevant_pages()
        if start_page_idx is None:
            return pd.DataFrame()
        trechos = []
        try:
            with pdfplumber.open(io.BytesIO(self.pdf_bytes)) as pdf:
                for i in range(start_page_idx, end_page_idx):
                    pagina = pdf.pages[i]
                    largura, altura = pagina.width, pagina.height
                    for col_num, (x0, x1) in enumerate([(0, largura/2), (largura/2, largura)], start=1):
                        coluna = pagina.crop((x0, 0, x1, altura)).extract_text(layout=True) or ""
                        texto_limpo = coluna.replace('\xa0', ' ')
                        trechos.append({
                            "pagina": i + 1,
                            "coluna": col_num,
                            "texto": texto_limpo
                        })
        except Exception as e:
            st.error(f"Erro ao extrair texto detalhado do PDF do Executivo: {e}")
            return pd.DataFrame()

        dados = []
        ultima_norma = None
        seen_alteracoes = set()
        for t in trechos:
            pagina = t["pagina"]
            coluna = t["coluna"]
            texto = t["texto"]
            eventos = []
            for m in self.norma_regex.finditer(texto):
                eventos.append(('published', m.start(), m))
            for c in self.comandos_regex.finditer(texto):
                eventos.append(('command', c.start(), c))
            eventos.sort(key=lambda e: e[1])
            for ev in eventos:
                tipo_ev, pos_ev, match_obj = ev
                command_text = match_obj.group(0).lower()
                if tipo_ev == 'published':
                    match = match_obj
                    tipo_raw = match.group(1).strip()
                    tipo = self.mapa_tipos.get(tipo_raw.upper(), tipo_raw)
                    numero = match.group(2).replace(" ", "").replace(".", "")
                    data_texto = (match.group(3) or "").strip()

                    data_match = re.search(
                        r'(\d{1,2})\s+DE\s+([A-ZÇÃÁÉÍÓÔÚ]+)\s+DE\s+(\d{4})',
                        data_texto,
                        re.IGNORECASE
                    )

                    if data_match:
                        dia = data_match.group(1).zfill(2)
                        mes_nome = data_match.group(2).upper()
                        mes = meses.get(mes_nome, "")
                        ano = data_match.group(3)
                        sancao = f"{dia}/{mes}/{ano}" if mes else ""
                    else:
                        sancao = ""
                    linha = {
                        "Página": pagina,
                        "Coluna": coluna,
                        "Sanção": sancao,
                        "Tipo": tipo,
                        "Número": numero,
                        "Alterações": ""
                    }
                    dados.append(linha)
                    ultima_norma = linha
                    seen_alteracoes = set()
                elif tipo_ev == 'command':
                    if ultima_norma is None:
                        continue
                    raio = 150
                    start_block = max(0, pos_ev - raio)
                    end_block = min(len(texto), pos_ev + raio)
                    bloco = texto[start_block:end_block]
                    alteracoes_para_processar = []
                    if 'revogado' in command_text:
                        alteracoes_para_processar = list(self.norma_alterada_regex.finditer(bloco))
                    else:
                        alteracoes_candidatas = list(self.norma_alterada_regex.finditer(bloco))
                        if alteracoes_candidatas:
                            pos_comando_no_bloco = pos_ev - start_block
                            melhor_candidato = min(
                                alteracoes_candidatas,
                                key=lambda m: abs(m.start() - pos_comando_no_bloco)
                            )
                            alteracoes_para_processar = [melhor_candidato]
                    for alt in alteracoes_para_processar:
                        tipo_alt_raw = alt.group(1).strip()
                        tipo_alt = self.mapa_tipos.get(tipo_alt_raw.upper(), tipo_alt_raw)
                        num_alt = alt.group(2).replace(" ", "").replace(".", "").replace("/", "")
                        data_texto_alt = alt.group(3)
                        ano_alt = ""
                        if data_texto_alt:
                            ano_match = re.search(r'(\d{4})', data_texto_alt)
                            if ano_match:
                                ano_alt = ano_match.group(1)
                        chave_alt = f"{tipo_alt} {num_alt}"
                        if ano_alt:
                            chave_alt += f" {ano_alt}"
                        if tipo_alt == ultima_norma["Tipo"] and num_alt == ultima_norma["Número"]:
                            continue
                        if chave_alt in seen_alteracoes:
                            continue
                        seen_alteracoes.add(chave_alt)
                        if ultima_norma["Alterações"] == "":
                            ultima_norma["Alterações"] = chave_alt
                        else:
                            dados.append({
                                "Página": "",
                                "Coluna": "",
                                "Sanção": "",
                                "Tipo": "",
                                "Número": "",
                                "Alterações": chave_alt
                            })
        return pd.DataFrame(dados) if dados else pd.DataFrame()

    def to_csv(self):
        df = self.process_pdf()
        if df.empty:
            return None
        output_csv = io.StringIO()
        df.to_csv(output_csv, index=False, encoding="utf-8-sig")
        return output_csv.getvalue().encode('utf-8')
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

    frames = []

    if not df_exec.empty:
        df_exec = df_exec.copy()
        df_exec["Origem"] = "Executivo"
        frames.append(df_exec)

    if not df_leg.empty:
        df_leg = df_leg.copy()
        df_leg = df_leg.rename(columns={"Sigla": "Tipo"})
        df_leg["Alterações"] = ""
        df_leg["Origem"] = "Legislativo"
        frames.append(df_leg)

    if frames:
        df_final = pd.concat(frames, ignore_index=True)

        data_out = [df_final.columns.tolist()] + df_final.values.tolist()
        sheet.update("A1", data_out)

        st.success(f"Planilha atualizada 🚀 ({len(df_final)} registros)")
    else:
        st.warning("Nada para enviar")

except Exception as e:
    st.error(f"Erro Google Sheets: {e}")
