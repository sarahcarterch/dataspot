import streamlit as st
from streamlit.components.v1 import iframe
from src.sarah.url import *
#checkliste, projektverzeichnis, ogd_freigaben, datenkatalog, datennutzungskatalog, datenportal

def einstieg():
    st.title("Workflow OGD-Freigaben")

    st.markdown("**Bei der Beurteilung, ob Daten öffentlicher Organe über ein Open Government Data-Portal veröffentlicht werden dürfen, "  \
    "sind aus datenschutzrechtlicher Sicht wichtige Punkte vorgängig zu prüfen. " \
    "Die Prüfung erfolgt durch *Data Stewards* und *Data Owner*. " \
    "Sie werden dabei von den *Kantonalen Data Stewards* des DCC Data Competencen Center unterstützt. " \
    "Die Prüfung erfolgt anhand einer Checkliste.**")
    st.write("---")
    st.markdown("#### **Anleitung für:**")
    st.markdown(''' 
    *Kantonale Data Stewards (DCC)*: Mitarbeitende des DCC mit Admin-Rechten  
    *Data Steward (DS)*: Personen mit Fachverantwortung für einen Datensatz  
    *Data Owner (DO)*: Dienststellenleitung, die den jeweiligen DS vorsteht  
    ''')

    with st.expander(label="Checkliste", expanded=False):
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
    st.write("---")
    st.markdown("#### **Rolle suchen:**")
    st.write("Bitte wählen Sie einen Namen.")
    name = st.selectbox(label="Ihr Name",label_visibility="hidden", options=["", "Hallo"], key="name", help="Bitte einen Namen wählen.")
    if name == "Hallo":
        st.write("Ihre Rolle:")
        st.success("Goodbye")
    else:
        st.write("")
        st.write("")
        st.write("Falls Ihr Name nicht in der Liste erscheint:")
        st.warning("Melden Sie sich bitte beim DCC, damit Sie im Datenkatalog erfasst werden.")
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
        with st.expander(label="Vorbereitungen durch DCC ", expanded=True):
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

    # Tab Datensatz im Datenkatalog
    with tabs[1]:
        
        st.subheader("Leeren Datensatz anlegen")
        st.info(f"Im Datenportal: {datenportal}")

    # Tab Leerer Datensatz im Datenportal
    with tabs[2]:
        st.subheader("Person erstellen")
        st.warning(f"Im Datenkatalog: {datenkatalog}")

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
    subpages = st.sidebar.radio("Bearbeitungs-Schritt wählen", ["Einstieg", "Vorbereitung","Erste Prüfung","Zweite Prüfung","Abschluss"])

    if subpages == "Einstieg":
        einstieg()
    elif subpages == "Vorbereitung":
        st.header("Vorbereitung durch DCC")
        dcc_vorb()
    elif subpages == "Erste Prüfung":
        st.header("Erste Prüfung durch Data Steward")
        ds_first()
    elif subpages == "Zweite Prüfung":
        st.header("Zweite Prüfung durch Data Owner")
        do_second()
    else:
        st.header("Abschluss durch DCC")
        dcc_end()

if __name__ == "__main__":
    main()