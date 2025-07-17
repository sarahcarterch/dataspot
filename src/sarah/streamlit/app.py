import streamlit as st
import pandas as pd
import numpy as np
from collections import defaultdict

import os
import logging
import argparse
from time import sleep

from src.ogd_client import OGDClient
from src.dataspot_auth import DataspotAuth
from src.common import email_helpers
import json
from src.common import requests_get

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

auth = DataspotAuth()

# Streamlit UI
st.set_page_config(
    page_title="Home",  # ← Titel im Browser-Tab
    layout="wide"
)

st.title("Workflow OGD-Freigaben")
st.header("Anleitung")

st.markdown("""**Datenschutzrechtliche Zulässigkeit der Veröffentlichung von Daten über das OGD-Portal**:  
Bei der Beurteilung, ob Daten öffentlicher Organe über ein Open Government Data-Portal ([OGD-Portal](https://data.bs.ch/pages/home/)) veröffentlicht werden dürfen, sind aus datenschutzrechtlicher Sicht die untenstehenden Punkte (Ziffern 1-3) vorgängig zu prüfen.  
1. Sind die Daten, die veröffentlicht werden sollen, Sachdaten?
2. Kann bei Personendaten, die veröffentlicht werden sollen, der Personenbezug irreversibel entfernt werden?
3. Ihre Verantwortung: Die Verantwortung für den Umgang mit Informationen trägt dasjenige öffentliche Organ, das die 
Informationen zur Erfüllung seiner gesetzlichen Aufgaben bearbeitet. (...)

#### Vollständige Checkliste:
https://data-bs.ch/stata/opendatabs/OGD-Checkliste.pdf

#### Hauptakteure
* *Kantonale Data Stewards (DCC)*: Mitarbeitende des DCC mit Admin-Rechten
* *Data Steward (DS)*: Fachverantwortung
* *Data Owner (DO)*: Dienststellenleitung

#### Freigabe-Ablauf

1. Überprüfung des zu veröffentlichenden Datensatzes (mithilfe der Checkliste) durch DS und DO
2. Erste Veröffentlichungs-Freigabe in Dataspot durch **DS**
3. Zweite Veröffentlichungs-Freigabe in Dataspot durch **DO**

#### Voraussetzungen
*(Durch DCC sicherzustellen)*
* DS und DO haben ein Konto beim [Datenkatalog](https://datenkatalog.bs.ch/) (Person und Benutzer:in erstellen, Link senden, Anmeldung sicherstellen)
* Freigabe-Projekt pro Datensatz im [Projektverzeichnis des Datenkatalogs](https://datenkatalog.bs.ch/web/prod/schemes/8386fbc7-2315-4d00-9bf8-47d2b04a6a7d) erstellt (durch DCC)
* *Leere* Datensätze mit Metadaten und ID sind als *Entwurf* im [Datenportal](daten.bs.ch) abgelegt
* Datensätze sind vom Datenportal in den [Datennutzungskatalog des Datenkatalogs](https://datenkatalog.bs.ch/web/prod/schemes/0f16581d-ddff-4815-a423-3628baa326cc) übertragen (damit sie mit den Projekten verknüpft werden können)

#### Freigabe-Projekte anlegen
1. In der Sammlung [OGD-Freigaben](https://datenkatalog.bs.ch/web/prod/schemes/8386fbc7-2315-4d00-9bf8-47d2b04a6a7d) unter der passenden Dienststelle in **neues Projekt** anlegen
2. Neue Projekte wie folgt benennen und bearbeiten:

*  **Bezeichnung für neue Datensätze**: *"Datensatz <ID aus dem Datenportal> anlegen und freigeben"*
*  **Bezeichnung für Mutationen an bestehenden Datensätzen**: z.B. *"Datensatz <ID aus dem Datenportal> mutieren"*
*  **Titel**: Datensatz-Titel wie im Datenportal
*  **Beschreibung**: Link zum Datensatz im [Datenportal](https://data.bs.ch/pages/home/)
*  **Verantwortlich**: Data Steward und Data Owner festlegen
*  **Verwendungen**: Verknüpfung zum Datennutzungskatalog herstellen""")