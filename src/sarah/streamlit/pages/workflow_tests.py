import streamlit as st
from streamlit.components.v1 import iframe
from src.sarah.url import * #checkliste, projektverzeichnis, ogd_freigaben, datenkatalog, datennutzungskatalog, datenportal
from src.sarah.streamlit.pages.personen_suchen import personen_liste, df
from src.sarah.streamlit.pages.zuweisungen_filtern import data
from src.sarah.create_person import erstelle_person


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
    tabs = st.tabs(["Einstieg", "Schritt 1", "Schritt 2", "Schritt 3", "Schritt 4", "Schritt 5", "Schritt 6"])

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
    with tabs[2]:
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
        givenName = st.text_input(label="Vorname", placeholder="Vornamen eingeben")
        familyName = st.text_input(label="Nachname", placeholder="Nachnamen eingeben")
        if st.button(label="Neue Person vorbereiten"):
            st.write(f"{givenName} {familyName} im Datenkatalog erfassen.")
        if st.button(label="Im Datenkatalog erfassen"):
            erstelle_person(family_name=familyName, given_name=givenName)
            st.success("Person erfolgreich erstellt.")

        st.write("#### 3. Hat Person Rolle in einem Projekt?")
        
        gesucht = st.text_input(label="Gesucht", label_visibility="hidden", placeholder="Gesuchte Person eingeben", key="second")        
        if gesucht:
            if gesucht in personen_liste:
                st.success("Person ist im Datenkatalog erfasst.")
            else:
                st.warning("Gesuchte Person im Datenkatalog nicht gefunden.")
        col1,col2 = st.columns(2)
        with col1:
            st.write("Projekt1") 
            st.write("Projekt2") 
            st.write("...")
        with col2:
            st.write("Rolle1") 
            st.write("Rolle2") 
            st.write("...")

        st.write("#### 4. Rolle in einem Projekt zuweisen")
        st.write("... folgt.")

    # Tab Benutzerkontos im Datenkatalog
    with tabs[3]:
        st.subheader("Benutzer:in erstellen")
        st.warning(f"Im Datenkatalog: {datenkatalog}")

    # Tab Projekt im Datenkatalog
    with tabs[4]:
        st.subheader("Anmeldung sicherstellen")
        st.success("Persönlich oder per Mail")

    # Tab Datensatz im Datenkatalog
    with tabs[5]:
        st.subheader("Projekt erstellen")
        st.warning(f"Im Datenkatalog: {datenkatalog}")
    
    # Tab Datensatz im Datenkatalog
    with tabs[6]:
        st.subheader("Datenprodukt erstellen")
        st.warning(f"Im Datenkatalog: {datenkatalog}")

def ds_first():
    # Tabs für Schritt 1 DCC: Vorbereitung
    tabs = st.tabs(["Einstieg", "Punkt 1", "Punkt 2", "Punkt 3", "Freigabe"])

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
        st.success("Workflow abgeschlossen.")

def do_second():
    # Tabs für Schritt 1 DCC: Vorbereitung
    tabs = st.tabs(["Einstieg", "Punkt 1", "Punkt 2", "Punkt 3", "Freigabe"])

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