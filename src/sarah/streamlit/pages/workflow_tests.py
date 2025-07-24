import streamlit as st
from streamlit.components.v1 import iframe
from src.sarah.url import * #checkliste, projektverzeichnis, ogd_freigaben, datenkatalog, datennutzungskatalog, datenportal
from src.sarah.streamlit.pages.personen_suchen import personen_liste, df
from src.sarah.streamlit.pages.zuweisungen_filtern import data
from src.sarah.create_person import erstelle_person
from src.sarah.create_account import erstelle_konto
from src.sarah.create_project_ogd import erstelle_projekt
from src.sarah.streamlit.pages.projekte_auflisten import projects
from src.common import requests_get
import pandas as pd
from src.dataspot_auth import *
import subprocess
import json
import os

auth = DataspotAuth()

# Daten von der API holen
@st.cache_data(show_spinner="Lade Konten...")
def lade_konten():
    base_url = "https://bs.dataspot.io"
    # Get Users
    endpoint_u = "/rest/test-sarah-1/users"
    response_u = requests_get(f"{base_url}{endpoint_u}", headers=auth.get_headers())
    data_u = response_u.json()
    
    return data_u.get("_embedded", {}).get("users", [])

def einstieg():
    st.title("Workflow OGD-Freigaben")

    st.markdown("**Bei der Beurteilung, ob Daten öffentlicher Organe über ein Open Government Data-Portal veröffentlicht werden dürfen, "  \
    "sind aus datenschutzrechtlicher Sicht wichtige Punkte vorgängig zu prüfen. " \
    "Die Prüfung erfolgt durch *Data Stewards* und *Data Owner*. " \
    "Sie werden dabei von den *Kantonalen Data Stewards* des DCC Data Competencen Center unterstützt. " \
    "Die Prüfung erfolgt anhand einer Checkliste.**")
    st.write("---")
    st.write("**Informationen**")
    st.info(''' 
    ***Kantonale Data Stewards (DCC)***: Mitarbeitende des DCC mit Admin-Rechten  
    ***Data Steward (DS)***: Personen mit Fachverantwortung für einen Datensatz  
    ***Data Owner (DO)***: Dienststellenleitende, die den jeweiligen DS vorstehen  
    ''')

    with st.expander(label="Link zur Checkliste", expanded=False):
        st.markdown(checkliste)

    with st.expander(label="Freigabe-Ablauf", expanded=False):
            st.markdown("""
    #### Freigabe-Ablauf

    1. Notwendige Vorbereitungen durch Kantonale Data Stewards (DCC)
    2. Überprüfung des zu veröffentlichenden Datensatzes durch Data Steward (DS) und Data Owner (DO)
    3. Erste Veröffentlichungs-Freigabe in Dataspot durch **DS**
    4. Zweite Veröffentlichungs-Freigabe in Dataspot durch **DO**
    5. Abschluss des Freigabe-Prozess und Veröffentlichung durch **DCC**
                        """)
    with st.expander(label="Vorbereitungen durch DCC ", expanded=False):
        st.markdown("""
    #### Vorbereitungen
    * DS und DO haben ein Konto beim [Datenkatalog]({datenkatalog}) (Person und Benutzer:in erstellen, Link senden, Anmeldung sicherstellen)
    * Freigabe-Projekt pro Datensatz im [Projektverzeichnis des Datenkatalogs]({projektverzeichnis}) erstellt
    * *Leere* Datensätze mit Metadaten und ID sind als *Entwurf* im [Datenportal](daten.bs.ch) abgelegt
    * Datensätze sind vom Datenportal in den [Datennutzungskatalog des Datenkatalogs]({datennutzungskatalog}) übertragen (damit sie mit den Projekten verknüpft werden können)
                        """)
        with st.expander(label="Freigabe-Projekt anlegen durch DCC", expanded=False):
                st.markdown("""
        #### Freigabe-Projekte anlegen
        1. In der Sammlung [OGD-Freigaben]({ogd_freigaben}) unter der passenden Dienststelle ein **neues Projekt** anlegen
        2. Das **neue Projekt** nach folgendem Schema benennen und bearbeiten:

            *  **Bezeichnung für neue Datensätze**: "Datensatz <ID aus dem Datenportal> anlegen und freigeben"
            *  **Bezeichnung für Mutationen an bestehenden Datensätzen**: z.B. "Datensatz <ID aus dem Datenportal> mutieren"
            *  **Titel**: Datensatz-Titel wie im Datenportal
            *  **Beschreibung**: Link zum Datensatz im [Datenportal]({datenportal}})
            *  **Verantwortlich**: Data Steward und Data Owner festlegen
            *  **Verwendungen**: Verknüpfung zum Datennutzungskatalog herstellen
                            """)

    # Die eigene Rolle suchen
    personen_liste_bereinigt = [p.strip() for p in personen_liste]
        # --- Rolle nach Name und Projekt suchen ---
    st.write("---")
    st.markdown("#### **Rolle nach Projekt suchen:**")

    # Name aus Liste auswählen
    name_input = st.selectbox("Bitte suchen Sie einen Namen", [""] + [f"{v.split(', ')[1]} {v.split(', ')[0]}" for v in data["persons"].values()], key="proj_name")

    # Projekt aus Liste auswählen
    projekt_input = st.selectbox("Bitte wählen Sie ein Projekt", [""] + list(data["projects"].values()), key="proj_project")

    rolle_auswahl = None

    if name_input and projekt_input:
        # Name umwandeln in "Nachname, Vorname"
        teile = name_input.strip().split(" ")
        if len(teile) >= 2:
            suchname = f"{' '.join(teile[1:])}, {teile[0]}"
        else:
            suchname = name_input.strip()

        # Person-ID finden
        person_id = next((pid for pid, pname in data["persons"].items() if pname.strip() == suchname), None)

        # Projekt-ID finden
        projekt_id = next((pid for pid, pname in data["projects"].items() if pname.strip() == projekt_input), None)

        # Rolle für genau dieses Projekt und diese Person suchen
        if person_id and projekt_id:
            projekt = data["attributions"].get(projekt_id, {})
            for eintrag in projekt.get("personen", []):
                if isinstance(eintrag, dict) and eintrag.get("person") == person_id:
                    rolle_id = eintrag.get("role")
                    rolle_auswahl = data["roles"].get(rolle_id)
                    break

    # Ergebnis anzeigen
    if rolle_auswahl:
        st.success(f"Ihre Rolle im Projekt: **{rolle_auswahl}**")
    elif name_input and projekt_input:
        st.warning("Für diese Kombination aus Name und Projekt wurde keine Rolle gefunden.")

    else:
        st.write("")
        st.write("")
        st.warning("**Falls Ihr Name oder Ihre Rolle nicht erscheint**: Melden Sie sich bitte beim DCC, damit Sie im Datenkatalog korrekt erfasst werden.")

    # Checklisten-PDF als Iframe einbetten
    st.write("---") # Trennlinie
    st.markdown("#### Vollständige Checkliste")
    pdf_url = checkliste
    if pdf_url:
        st.markdown(
            f'<iframe src="https://docs.google.com/gview?url={pdf_url}&embedded=true" width="80%" height="800px" frameborder="0"></iframe>', 
            unsafe_allow_html=True)
    st.write("---") # Trennlinie

def dcc_vorb():
    # Tabs für Schritt 1 DCC: Vorbereitung
    tabs = st.tabs(["Einstieg", "Schritt 1", "Schritt 2", "Schritt 3", "Schritt 4", "Schritt 5", "Schritt 6", "Schritt 7"])

    # Tab Einstieg
    with tabs[0]:
        with st.expander(label="Freigabe-Ablauf", expanded=False):
            st.markdown("""
    #### Freigabe-Ablauf

    1. Notwendige Vorbereitungen durch Kantonale Data Stewards (DCC)
    2. Überprüfung des zu veröffentlichenden Datensatzes durch Data Steward (DS) und Data Owner (DO)
    3. Erste Veröffentlichungs-Freigabe in Dataspot durch **DS**
    4. Zweite Veröffentlichungs-Freigabe in Dataspot durch **DO**
    5. Abschluss des Freigabe-Prozess und Veröffentlichung durch **DCC**
                        """)
        st.subheader("Einstieg")
        with st.expander(label="Vorbereitungen durch DCC ", expanded=False):
            st.markdown("""
        * DS und DO haben ein Konto beim [Datenkatalog]({datenkatalog}) (Person und Benutzer:in erstellen, Link senden, Anmeldung sicherstellen)
        * Freigabe-Projekt pro Datensatz im [Projektverzeichnis des Datenkatalogs]({projektverzeichnis}) erstellt
        * *Leere* Datensätze mit Metadaten und ID sind als *Entwurf* im [Datenportal](daten.bs.ch) abgelegt
        * Datensätze sind vom Datenportal in den [Datennutzungskatalog des Datenkatalogs]({datennutzungskatalog}) übertragen (damit sie mit den Projekten verknüpft werden können)
                            """)
            with st.expander(label="Freigabe-Projekt anlegen durch DCC", expanded=False):
                    st.markdown("""
            #### Freigabe-Projekte anlegen
            1. In der Sammlung [OGD-Freigaben]({ogd_freigaben}) unter der passenden Dienststelle ein **neues Projekt** anlegen
            2. Das **neue Projekt** nach folgendem Schema benennen und bearbeiten:

                *  **Bezeichnung für neue Datensätze**: "Datensatz <ID aus dem Datenportal> anlegen und freigeben"
                *  **Bezeichnung für Mutationen an bestehenden Datensätzen**: z.B. "Datensatz <ID aus dem Datenportal> mutieren"
                *  **Titel**: Datensatz-Titel wie im Datenportal
                *  **Beschreibung**: Link zum Datensatz im [Datenportal]({datenportal}})
                *  **Verantwortlich**: Data Steward und Data Owner festlegen
                *  **Verwendungen**: Verknüpfung zum Datennutzungskatalog herstellen
                                """)

    # Tab Leeren Datensatz anlegen
    with tabs[1]:
        
        st.subheader("Leeren Datensatz anlegen")
        st.info(f"Im Datenportal: {datenportal}")

    # Tab Person erstellen
    with tabs[4]:
        st.subheader("Person erstellen")
        st.warning(f"Im Datenkatalog: {datenkatalog}")

        st.write("#### 1. Ist Person in Dataspot erfasst?")
        gesucht = st.text_input(label="Gesucht", label_visibility="hidden", placeholder="Gesuchte Person eingeben", key="first")        
        if gesucht:
            if gesucht in personen_liste:
                st.success("Person ist im Datenkatalog erfasst.")
            else:
                st.warning("Gesuchte Person im Datenkatalog nicht gefunden.")

        st.write("#### 2. Fehlende Personen erfassen")
        givenName = st.text_input(label="Vorname", label_visibility="hidden", placeholder="Vornamen eingeben")
        familyName = st.text_input(label="Nachname", label_visibility="hidden", placeholder="Nachnamen eingeben")
        if st.button(label="Neue Person vorbereiten"):
            st.write(f"{givenName} {familyName} im Datenkatalog erfassen.")
        if st.button(label="Im Datenkatalog erfassen"):
            erstelle_person(family_name=familyName, given_name=givenName)
            st.success("Person erfolgreich erstellt.")

    # Tab Benutzerkontos im Datenkatalog
    with tabs[5]:
        st.subheader("Benutzer:in erstellen")
        st.warning(f"Im Datenkatalog: {datenkatalog}")

        st.write("#### 1. Hat Person in Dataspot ein Benutzerkonto?")

        konten = lade_konten()
        dfk = pd.DataFrame(konten)
        # Left merge von df und dfk
        konten_liste = (
            df[["id", "name"]].dropna().drop_duplicates()
            .merge(dfk[["isPerson", "loginId", "accessLevel"]], left_on="id", right_on="isPerson", how="inner")
            .query("loginId.notnull()")
            .sort_values("name")
            .to_dict(orient="records")
        )
        for eintrag in konten_liste:
            eintrag.pop("isPerson", None)
        print(f"Felder: {konten_liste[0].keys()}")

        name_liste = [eintrag["name"] for eintrag in konten_liste if "name" in eintrag]


        gesucht_konto = st.text_input(label="Gesuchtes Konto", label_visibility="hidden", placeholder="Gesuchtes Konto eingeben", key="account")
        
        
        if gesucht_konto in personen_liste:
            if gesucht_konto:
                if gesucht_konto in name_liste:
                    person_obj = next((eintrag for eintrag in konten_liste if eintrag.get("name") == gesucht_konto), None)
                    person_id = person_obj.get("id") if person_obj else None
                    st.success("Person ist im Datenkatalog erfasst.")
                    st.success(f"Person hat im Datenkatalog ein Benutzerkonto.")
                else:
                    st.success("Person ist im Datenkatalog erfasst.")
                    st.warning("Benutzerkonto im Datenkatalog nicht gefunden.")
            else:
                st.warning("Gesuchte Person im Datenkatalog nicht gefunden.")

        st.write("#### 2. Benutzerkonto erfassen")
        email = st.text_input(label="Email", label_visibility="hidden", placeholder="Email für Login-Erstellung eingeben", key="email")
        if st.button("Neues Konto vorbereiten", key="btn_neue_person_vorbereiten"):
            st.write(f"{gesucht_konto} mit Login {email} im Datenkatalog erfassen.")

        if st.button("Im Datenkatalog erfassen", key="btn_im_datenkatalog_erfassen"):
            erstelle_konto(name=gesucht_konto, loginId=email)
            st.success("Konto erfolgreich erstellt.")
        
    # Tab Anmeldung sicherstellen
    with tabs[7]:
        st.subheader("Anmeldung sicherstellen")
        st.success("Persönlich oder per Mail")

    # Tab Projekt in Verzeichnis OGD-Freigaben erstellen
    with tabs[2]:
        st.subheader("Projekt erstellen")
        st.warning(f"Im Datenkatalog: {datenkatalog}")

        st.write("#### 1. Ist Projekt im Projektverzeichnis *OGD-Freigaben* erfasst?")
        projects_label = [eintrag["label"] for eintrag in projects]

        project_list = st.selectbox(label="Projekt", label_visibility="hidden", placeholder="Gesuchtes Projekt eingeben", key="proj", options=[""]+projects_label, index=0)        

        st.write("#### 2. Falls nicht: Neues Projekt erfassen")
        label = st.text_input(label="Projekttitel", label_visibility="hidden", placeholder="Neuen Titel eingeben")
        if st.button(label="Neues Projekt vorbereiten", key="neues_proj_vorb"):
            st.write(f"{label} im Datenkatalog erfassen.")
        if st.button(label="Im Datenkatalog erfassen", key="neues_proj_erf"):
            erstelle_projekt(label=label)
            st.success("Projekt erfolgreich erstellt.")

    # Tab Datensatz im Datenkatalog
    with tabs[3]:
        st.subheader("Datenprodukt erstellen")
        st.warning(f"Im Datenkatalog: {datenkatalog}")
    
    # Tab Rolle in einem Projekt zuweisen
    with tabs[6]:
        st.subheader("Rollen in einem Projekt zuweisen")
        st.warning(f"Im Datenkatalog: {datenkatalog}")

        # Get current data from API
        # Button zum Aktualisieren hinzufügen
        if st.button("Daten neu laden"):
            subprocess.run(["python","src\sarah\project_attributions.py"])

        # JSON-Datei laden
        json_path = "attributions.json"
        if not os.path.exists(json_path):
            st.error("Datei attributions.json nicht gefunden.")
            st.stop()

        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Projekte extrahieren
        projekt_mapping = {
            pid: label
            for pid, label in data.get("projects", {}).items()
            if (
                isinstance(data.get("attributions", {}).get(pid), dict)
                and data["attributions"][pid].get("_type") == "Project"
            )
        }
        projekt_namen = sorted(projekt_mapping.values())
        projekt_id_lookup = {v: k for k, v in projekt_mapping.items()}

        # Projektwahl
        gewähltes_projekt = st.selectbox(label = "Projekt auswählen", options=[""]+projekt_namen, index=0)

        if gewähltes_projekt:
            projekt_id = projekt_id_lookup[gewähltes_projekt]
            projekt_eintrag = data["attributions"][projekt_id]
            aktuelle_attributions = projekt_eintrag.get("personen", [])

            # Sicherstellen, dass Attributions existieren
            if projekt_id not in data["attributions"]:
                data["attributions"][projekt_id] = {
                    "status": "",
                    "personen": []
                }

            projekt_eintrag = data["attributions"][projekt_id]

            # Optionen
            # Aus JSON-Datei (bestehende Zuweisungen)
            personen_liste_ed = df[["id", "name"]].dropna().drop_duplicates().to_dict(orient="records")

            json_persons = data.get("persons", {})
            name_to_id_json = {v: k for k, v in json_persons.items()}

            # Aus personen_liste
            name_to_id_neu = {p["name"]: p["id"] for p in personen_liste_ed}
            id_to_name_neu = {p["id"]: p["name"] for p in personen_liste_ed}

            # Alles kombinieren
            name_to_person_id = {**name_to_id_neu, **name_to_id_json}
            person_id_to_name = {v: k for k, v in name_to_person_id.items()}
            person_names = sorted(name_to_person_id.keys())

            role_options = data.get("roles", {})
            role_names = list(role_options.values())
            name_to_role_id = {v: k for k, v in role_options.items()}
            role_id_to_name = {k: v for k, v in role_options.items()}

            st.subheader("Zugewiesene Personen")

            neue_attributions = []
            num_rows = len(aktuelle_attributions) + 1  # eine zusätzliche Zeile zum Hinzufügen

            for i in range(num_rows):
                existing = aktuelle_attributions[i] if i < len(aktuelle_attributions) else {}
                selected_person = person_id_to_name.get(existing.get("person", ""), "")
                selected_role = role_id_to_name.get(existing.get("role", ""), "")

                col1, col2 = st.columns(2)
                with col1:
                    p = st.selectbox(
                        f"Person {i+1}",
                        options=[""] + person_names,
                        index=([""] + person_names).index(selected_person) if selected_person else 0,
                        key=f"person_{i}"
                    )
                with col2:
                    r = st.selectbox(
                        f"Rolle {i+1}",
                        options=[""] + role_names,
                        index=([""] + role_names).index(selected_role) if selected_role else 0,
                        key=f"rolle_{i}"
                    )

                if p and r:
                    neue_attributions.append({
                        "person": name_to_person_id[p],
                        "role": name_to_role_id[r]
                    })

            if st.button("Änderungen speichern"):
                if isinstance(data["attributions"][projekt_id], dict):
                    data["attributions"][projekt_id]["personen"] = neue_attributions
                else:
                    st.error("Fehler: Die Attributions-Struktur ist nicht wie erwartet.")
                    st.stop()
                with open(json_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                st.success("Änderungen gespeichert.")

        def upload_data():
            script_path = os.path.join("src", "sarah", "project_attr_sync.py")

            result = subprocess.run(
                ["python", script_path],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                st.cache_data.clear()
                st.success("Daten wurden erfolgreich hochgeladen.")
                st.text(result.stdout)
            else:
                st.error("Fehler beim Upload der Daten:")
                st.text(result.stderr)

        # Button zum Starten der Synchronisation
        st.write("---")
        if st.button("Synchronisation mit Datenplattform starten"):
            upload_data()

def ds_first():
    # Tabs für Schritt 1 DCC: Vorbereitung
    tabs = st.tabs(["Einstieg", "Punkt 1", "Punkt 2", "Punkt 3", "Freigabe"])

    # === Dateien laden ===
    json_path = "attributions.json"
    workflow_path = "workflow_ogd_stati.json"

    if not os.path.exists(json_path) or not os.path.exists(workflow_path):
        st.error("Benötigte Datei fehlt.")
        st.stop()

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    with open(workflow_path, "r", encoding="utf-8") as f:
        status_liste = json.load(f)

    # Tab Einstieg
    with tabs[0]:
        with st.expander(label="Freigabe-Ablauf", expanded=False):
            st.markdown("""
    #### Freigabe-Ablauf

    1. Notwendige Vorbereitungen durch Kantonale Data Stewards (DCC)
    2. Überprüfung des zu veröffentlichenden Datensatzes durch Data Steward (DS) und Data Owner (DO)
    3. Erste Veröffentlichungs-Freigabe in Dataspot durch **DS**
    4. Zweite Veröffentlichungs-Freigabe in Dataspot durch **DO**
    5. Abschluss des Freigabe-Prozess und Veröffentlichung durch **DCC**
                        """)
        st.subheader("Projekt auswählen")

        # Projekte extrahieren
        projekt_mapping = {
            pid: label
            for pid, label in data.get("projects", {}).items()
            if (
                isinstance(data.get("attributions", {}).get(pid), dict)
                and data["attributions"][pid].get("_type") == "Project"
            )
        }
        projekt_namen = sorted(projekt_mapping.values())
        projekt_id_lookup = {v: k for k, v in projekt_mapping.items()}

        # Projektwahl mit leerer Vorauswahl
        gewähltes_projekt = st.selectbox(
            label="Projekt auswählen",
            options=[""] + projekt_namen,
            index=0,
            label_visibility="hidden"
        )

        if gewähltes_projekt:
            projekt_id = projekt_id_lookup[gewähltes_projekt]
            projekt_eintrag = data["attributions"][projekt_id]
            aktuelle_attributions = projekt_eintrag.get("personen", [])

            # Mapping zur Anzeige der Rollen und Namen
            person_id_to_name = data.get("persons", {})
            role_id_to_name = data.get("roles", {})

            st.markdown("#### Zugewiesene Personen")

            if aktuelle_attributions:
                for eintrag in aktuelle_attributions:
                    person_id = eintrag.get("person")
                    role_id = eintrag.get("role")

                    person_name = person_id_to_name.get(person_id, f"(unbekannt: {person_id})")
                    role_name = role_id_to_name.get(role_id, f"(unbekannt: {role_id})")

                    st.write(f"- **{person_name}** als **{role_name}**")
            else:
                st.info("Diesem Projekt sind noch keine Personen mit Rollen zugewiesen.")
  
    # Tab Frage 1
    with tabs[1]:
        col1, col2 = st.columns(2)
        with col1:
            st.warning("Bitte prüfen")
            st.markdown("### Sind die Daten, die veröffentlicht werden sollen, Sachdaten?")
            # Prüfung
            st.text_area(label="Kommentar:", key="comm_1a")
            st.checkbox("Frage ist geprüft", key="frage_1a")
        with col2:
            pdf_url = checkliste
            if pdf_url:
                st.markdown(
                    f'<iframe src="https://docs.google.com/gview?url={pdf_url}&embedded=true" width="100%" height="800px" frameborder="0"></iframe>', 
                    unsafe_allow_html=True)

    # Tab Frage 2
    with tabs[2]:
        col1, col2 = st.columns(2)
        with col1:
            st.warning("Bitte prüfen")
            st.markdown("### Kann bei Personendaten, die veröffentlicht werden sollen, der Personenbezug irreversibel entfernt werden?")
            # Prüfung
            st.text_area(label="Kommentar:", key="comm_2a")
            st.checkbox("Frage ist geprüft", key="frage_2a")
        with col2:
            pdf_url = checkliste
            if pdf_url:
                st.markdown(
                    f'<iframe src="https://docs.google.com/gview?url={pdf_url}&embedded=true" width="100%" height="800px" frameborder="0"></iframe>', 
                    unsafe_allow_html=True)

    # Tab Frage 3
    with tabs[3]:
        col1, col2 = st.columns(2)
        with col1:
            st.warning("Bitte prüfen")
            st.markdown("### Ihre Verantwortung")
            st.write("Die Verantwortung für den Umgang mit Informationen trägt dasjenige öffentliche Organ, " \
            "das die Informationen zur Erfüllung seiner gesetzlichen Aufgaben bearbeitet. " \
            "Dieses öffentliche Organ (bzw. die Leitung dieses öffentlichen Organs) trägt auch dafür die Verantwortung, " \
            "dass es die (datenschutz-)rechtlichen Vorgaben einhält, also - im Open Government Data-Umfeld - " \
            "keine Personendaten bekannt gibt (und andere allfällig einer Veröffentlichung entgegenstehende öffentliche oder private Interessen berücksichtigt). " \
            "Das «Restrisiko» der Re-Identifikation von anonymisierten Personendaten hat deshalb dieses Organ zu übernehmen. " \
            "Dabei kommen nicht nur allfällige Schadenersatz- und Genugtuungsforderungen in Frage, " \
            "sondern es ist auch die Gefahr des Reputationsverlustes zu beachten. Das ist beim Entscheid über Eignung von Daten zur Veröffentlichung " \
            "über das Open Government Data-Portal zu berücksichtigen.")
            # Prüfung
            st.text_area(label="Kommentar:", key="comm_3a")
            st.checkbox("Frage ist geprüft", key="frage_3a")
        with col2:
            pdf_url = checkliste
            if pdf_url:
                st.markdown(
                    f'<iframe src="https://docs.google.com/gview?url={pdf_url}&embedded=true" width="100%" height="800px" frameborder="0"></iframe>', 
                    unsafe_allow_html=True)

    # Tab Abschluss
    with tabs[4]:
        st.subheader("Erste Freigabestufe")

        # Mapping für schnellen Zugriff: {"WORKING": "In Entwurf", ...}
        status_labels = {entry["status"]: entry["label"] for entry in status_liste}

        # Reine Statuswerte für Dropdown
        status_options = [entry["status"] for entry in status_liste]

        # === Projekt-Daten vorbereiten ===
        projekt_mapping = data.get("projects", {})
        attributions = data.get("attributions", {})

        projekt_namen = sorted(projekt_mapping.values())
        projekt_id_lookup = {v: k for k, v in projekt_mapping.items()}
        gewähltes_projekt = st.selectbox("Projekt auswählen", projekt_namen)

        if gewähltes_projekt:
            projekt_id = projekt_id_lookup[gewähltes_projekt]
            aktueller_status = attributions.get(projekt_id, {}).get("status", None)

            st.markdown(f"**Aktueller Status:** `{aktueller_status}` - **{status_labels.get(aktueller_status, 'Unbekannt')}**")

            neuer_status = st.selectbox(
            "Neuen Status wählen",
            options=status_options,
            format_func=lambda x: status_labels.get(x, x),
            index=status_options.index(aktueller_status) if aktueller_status in status_options else 0
        )

            if st.button("Status aktualisieren und hochladen"):
                if projekt_id in attributions:
                    attributions[projekt_id]["status"] = neuer_status
                else:
                    st.warning("Projekt nicht in attributions.json gefunden – wird neu angelegt.")
                    attributions[projekt_id] = {"status": neuer_status, "personen": []}
                    st.json(attributions[projekt_id])

                data["attributions"] = attributions
                with open(json_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)

                result = subprocess.run(["python", "src/sarah/project_attr_sync.py", projekt_id], capture_output=True, text=True)
                if result.returncode == 0:
                    st.success("Status gespeichert und zur API übertragen.")
                else:
                    # st.error(f"Fehler beim Sync: {result.stderr}")
                    st.error(f"Fehler beim Sync")

            if st.button("Aktuelle Daten von API laden"):
                result = subprocess.run(["python", "src/sarah/project_attributions.py"], capture_output=True, text=True)
                if result.returncode == 0:
                    st.success("Daten erfolgreich neu geladen. Bitte Seite neu laden.")
                else:
                    st.error(f"Fehler beim Neuladen: {result.stderr}")
    
        st.success("Workflow abgeschlossen.")

def do_second():
    # Tabs für Schritt 1 DCC: Vorbereitung
    tabs = st.tabs(["Einstieg", "Punkt 1", "Punkt 2", "Punkt 3", "Freigabe"])

    # === Dateien laden ===
    json_path = "attributions.json"
    workflow_path = "workflow_ogd_stati.json"

    if not os.path.exists(json_path) or not os.path.exists(workflow_path):
        st.error("Benötigte Datei fehlt.")
        st.stop()

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    with open(workflow_path, "r", encoding="utf-8") as f:
        status_liste = json.load(f)

    # Tab Einstieg
    with tabs[0]:
        with st.expander(label="Freigabe-Ablauf", expanded=False):
            st.markdown("""
    #### Freigabe-Ablauf

    1. Notwendige Vorbereitungen durch Kantonale Data Stewards (DCC)
    2. Überprüfung des zu veröffentlichenden Datensatzes durch Data Steward (DS) und Data Owner (DO)
    3. Erste Veröffentlichungs-Freigabe in Dataspot durch **DS**
    4. Zweite Veröffentlichungs-Freigabe in Dataspot durch **DO**
    5. Abschluss des Freigabe-Prozess und Veröffentlichung durch **DCC**
                        """)
        st.subheader("Projekt auswählen")

        # Projekte extrahieren
        projekt_mapping = {
            pid: label
            for pid, label in data.get("projects", {}).items()
            if (
                isinstance(data.get("attributions", {}).get(pid), dict)
                and data["attributions"][pid].get("_type") == "Project"
            )
        }
        projekt_namen = sorted(projekt_mapping.values())
        projekt_id_lookup = {v: k for k, v in projekt_mapping.items()}

        # Projektwahl mit leerer Vorauswahl
        gewähltes_projekt = st.selectbox(
            label="Projekt auswählen",
            options=[""] + projekt_namen,
            index=0,
            label_visibility="hidden"
        )

        if gewähltes_projekt:
            projekt_id = projekt_id_lookup[gewähltes_projekt]
            projekt_eintrag = data["attributions"][projekt_id]
            aktuelle_attributions = projekt_eintrag.get("personen", [])

            # Mapping zur Anzeige der Rollen und Namen
            person_id_to_name = data.get("persons", {})
            role_id_to_name = data.get("roles", {})

            st.markdown("#### Zugewiesene Personen")

            if aktuelle_attributions:
                for eintrag in aktuelle_attributions:
                    person_id = eintrag.get("person")
                    role_id = eintrag.get("role")

                    person_name = person_id_to_name.get(person_id, f"(unbekannt: {person_id})")
                    role_name = role_id_to_name.get(role_id, f"(unbekannt: {role_id})")

                    st.write(f"- **{person_name}** als **{role_name}**")
            else:
                st.info("Diesem Projekt sind noch keine Personen mit Rollen zugewiesen.")
        
    # Tab Frage 1
    with tabs[1]:
        col1, col2 = st.columns(2)
        with col1:
            st.warning("Bitte prüfen")
            st.markdown("### Sind die Daten, die veröffentlicht werden sollen, Sachdaten?")
            # Prüfung
            st.text_area(label="Kommentar:", key="comm_1a")
            st.checkbox("Frage ist geprüft", key="frage_1a")
        with col2:
            pdf_url = checkliste
            if pdf_url:
                st.markdown(
                    f'<iframe src="https://docs.google.com/gview?url={pdf_url}&embedded=true" width="100%" height="800px" frameborder="0"></iframe>', 
                    unsafe_allow_html=True)

    # Tab Frage 2
    with tabs[2]:
        col1, col2 = st.columns(2)
        with col1:
            st.warning("Bitte prüfen")
            st.markdown("### Kann bei Personendaten, die veröffentlicht werden sollen, der Personenbezug irreversibel entfernt werden?")
            # Prüfung
            st.text_area(label="Kommentar:", key="comm_2a")
            st.checkbox("Frage ist geprüft", key="frage_2a")
        with col2:
            pdf_url = checkliste
            if pdf_url:
                st.markdown(
                    f'<iframe src="https://docs.google.com/gview?url={pdf_url}&embedded=true" width="100%" height="800px" frameborder="0"></iframe>', 
                    unsafe_allow_html=True)

    # Tab Frage 3
    with tabs[3]:
        col1, col2 = st.columns(2)
        with col1:
            st.warning("Bitte prüfen")
            st.markdown("### Ihre Verantwortung")
            st.write("Die Verantwortung für den Umgang mit Informationen trägt dasjenige öffentliche Organ, " \
            "das die Informationen zur Erfüllung seiner gesetzlichen Aufgaben bearbeitet. " \
            "Dieses öffentliche Organ (bzw. die Leitung dieses öffentlichen Organs) trägt auch dafür die Verantwortung, " \
            "dass es die (datenschutz-)rechtlichen Vorgaben einhält, also - im Open Government Data-Umfeld - " \
            "keine Personendaten bekannt gibt (und andere allfällig einer Veröffentlichung entgegenstehende öffentliche oder private Interessen berücksichtigt). " \
            "Das «Restrisiko» der Re-Identifikation von anonymisierten Personendaten hat deshalb dieses Organ zu übernehmen. " \
            "Dabei kommen nicht nur allfällige Schadenersatz- und Genugtuungsforderungen in Frage, " \
            "sondern es ist auch die Gefahr des Reputationsverlustes zu beachten. Das ist beim Entscheid über Eignung von Daten zur Veröffentlichung " \
            "über das Open Government Data-Portal zu berücksichtigen.")
            # Prüfung
            st.text_area(label="Kommentar:", key="comm_3a")
            st.checkbox("Frage ist geprüft", key="frage_3a")
        with col2:
            pdf_url = checkliste
            if pdf_url:
                st.markdown(
                    f'<iframe src="https://docs.google.com/gview?url={pdf_url}&embedded=true" width="100%" height="800px" frameborder="0"></iframe>', 
                    unsafe_allow_html=True)

    # Tab Abschluss
    with tabs[4]:
        st.subheader("Zweite Freigabestufe")

        # Mapping für schnellen Zugriff: {"WORKING": "In Entwurf", ...}
        status_labels = {entry["status"]: entry["label"] for entry in status_liste}

        # Reine Statuswerte für Dropdown
        status_options = [entry["status"] for entry in status_liste]

        # === Projekt-Daten vorbereiten ===
        projekt_mapping = data.get("projects", {})
        attributions = data.get("attributions", {})

        projekt_namen = sorted(projekt_mapping.values())
        projekt_id_lookup = {v: k for k, v in projekt_mapping.items()}
        gewähltes_projekt = st.selectbox("Projekt auswählen", projekt_namen)

        if gewähltes_projekt:
            projekt_id = projekt_id_lookup[gewähltes_projekt]
            aktueller_status = attributions.get(projekt_id, {}).get("status", None)

            st.markdown(f"**Aktueller Status:** `{aktueller_status}` - **{status_labels.get(aktueller_status, 'Unbekannt')}**")

            neuer_status = st.selectbox(
            "Neuen Status wählen",
            options=status_options,
            format_func=lambda x: status_labels.get(x, x),
            index=status_options.index(aktueller_status) if aktueller_status in status_options else 0
        )

            if st.button("Status aktualisieren und hochladen"):
                if projekt_id in attributions:
                    attributions[projekt_id]["status"] = neuer_status
                else:
                    st.warning("Projekt nicht in attributions.json gefunden – wird neu angelegt.")
                    attributions[projekt_id] = {"status": neuer_status, "personen": []}
                    st.json(attributions[projekt_id])

                data["attributions"] = attributions
                with open(json_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)

                result = subprocess.run(["python", "src/sarah/project_attr_sync.py", projekt_id], capture_output=True, text=True)
                if result.returncode == 0:
                    st.success("Status gespeichert und zur API übertragen.")
                else:
                    # st.error(f"Fehler beim Sync: {result.stderr}")
                    st.error(f"Fehler beim Sync")

            if st.button("Aktuelle Daten von API laden"):
                result = subprocess.run(["python", "src/sarah/project_attributions.py"], capture_output=True, text=True)
                if result.returncode == 0:
                    st.success("Daten erfolgreich neu geladen. Bitte Seite neu laden.")
                else:
                    st.error(f"Fehler beim Neuladen: {result.stderr}")
    
        st.success("Workflow abgeschlossen.")

def dcc_end():
    # Tabs für Schritt 1 DCC: Vorbereitung
    tabs = st.tabs(["Freigaben überprüfen", "Einwilligungen ablegen", "Datensatz publizieren", "Abschluss"])

    # Tab Einstieg
    with tabs[0]:
        with st.expander(label="Freigabe-Ablauf", expanded=False):
            st.markdown("""
    #### Freigabe-Ablauf

    1. Notwendige Vorbereitungen durch Kantonale Data Stewards (DCC)
    2. Überprüfung des zu veröffentlichenden Datensatzes durch Data Steward (DS) und Data Owner (DO)
    3. Erste Veröffentlichungs-Freigabe in Dataspot durch **DS**
    4. Zweite Veröffentlichungs-Freigabe in Dataspot durch **DO**
    5. Abschluss des Freigabe-Prozess und Veröffentlichung durch **DCC**
                        """)
        st.subheader("Freigaben überprüfen")
        st.info("Hier können Benutzereingaben erfasst oder Dateien hochgeladen werden.")
        uploaded_file = st.file_uploader("Datei hochladen", type=["csv", "xlsx"], key="upload_4")
        if uploaded_file:
            st.success("Datei erfolgreich hochgeladen.")
        

    # Tab Leerer Datensatz im Datenportal
    with tabs[1]:
        st.subheader("Einwilligungen ablegen")
        st.warning("Hier erfolgt die Prüfung der Eingabedaten.")
        # Beispielhafte Validierung
        st.checkbox("Formatprüfung abgeschlossen", key="check_4a")
        st.checkbox("Inhaltliche Prüfung abgeschlossen", key="check_4b")

    # Tab Benutzerkontos im Datenkatalog
    with tabs[2]:
        st.subheader("Datensatz publizieren")
        st.info("Verarbeite die validierten Daten.")
        if st.button("Verarbeitung starten", key="end_1"):
            with st.spinner("Verarbeite..."):
                # Hier folgt deine Logik
                st.success("Verarbeitung abgeschlossen.")

    # Tab Projekt im Datenkatalog
    with tabs[3]:
        st.subheader("Abschluss")
        st.download_button("Ergebnis herunterladen", data="Ergebnisdaten...", file_name="ergebnis.txt", key="end_2")

#Mögliche Erweiterungen:
#- Pro Tab eigene Funktionen auslagern, z.B. workflow_step_1(), wenn die Logik umfangreicher wird.
#- Für Zustandsverwaltung über Schritte hinweg st.session_state verwenden.
#- Auf „Zurück“- oder „Weiter“-Buttons wechseln (anstelle von Tabs).
#- Direkt Verzweigungen im Workflow (z.B. je nach Auswahl einen anderen Pfad)

def main():
    st.set_page_config(page_title="Workflow", layout="wide")

    # Radio-Buttons für die Workflow-Schritte
    subpages = st.sidebar.radio("Bearbeitungs-Schritt wählen", ["Einstieg", "Vorbereitung (DCC)","Erste Prüfung (Data Steward)","Zweite Prüfung (Data Owner)","Abschluss (DCC)"])

    if subpages == "Einstieg":
        einstieg()
    elif subpages == "Vorbereitung (DCC)":
        st.header("Vorbereitung durch DCC")
        dcc_vorb()
    elif subpages == "Erste Prüfung (Data Steward)":
        st.header("Erste Prüfung durch Data Steward")
        ds_first()
    elif subpages == "Zweite Prüfung (Data Owner)":
        st.header("Zweite Prüfung durch Data Owner")
        do_second()
    else:
        st.header("Abschluss durch DCC")
        dcc_end()

if __name__ == "__main__":
    main()