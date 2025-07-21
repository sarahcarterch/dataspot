import streamlit as st

def dcc_vorb():
    # Tabs für Schritt 1 DCC: Vorbereitung
    tabs = st.tabs(["Einstieg", "Leerer Datensatz im Datenportal", "Benutzerkontos im Datenkatalog", "Projekt im Datenkatalog", "Datensatz im Datenkatalog"])

    # Tab Einstieg
    with tabs[0]:
        st.subheader("Einstieg")
        st.markdown("- DS und DO haben ein Konto beim [Datenkatalog](https://datenkatalog.bs.ch/) (Person und Benutzer:in erstellen, Link senden, Anmeldung sicherstellen)")
        st.markdown("- Freigabe-Projekt pro Datensatz im [Projektverzeichnis des Datenkatalogs](https://datenkatalog.bs.ch/web/prod/schemes/8386fbc7-2315-4d00-9bf8-47d2b04a6a7d) erstellt (durch DCC)")
        st.markdown("- Leere Datensätze mit Metadaten und ID sind als Entwurf im [Datenportal](http://localhost:8501/daten.bs.ch) abgelegt")
        st.markdown("- Datensätze sind vom Datenportal in den [Datennutzungskatalog des Datenkatalogs](https://datenkatalog.bs.ch/web/prod/schemes/0f16581d-ddff-4815-a423-3628baa326cc) übertragen (damit sie mit den Projekten verknüpft werden können)")
        st.info("Hier können Benutzereingaben erfasst oder Dateien hochgeladen werden.")
        uploaded_file = st.file_uploader("Datei hochladen", type=["csv", "xlsx"])
        if uploaded_file:
            st.success("Datei erfolgreich hochgeladen.")
        

    # Tab Leerer Datensatz im Datenportal
    with tabs[1]:
        st.subheader("Leerer Datensatz im Datenportal")
        st.warning("Hier erfolgt die Prüfung der Eingabedaten.")
        # Beispielhafte Validierung
        st.checkbox("Formatprüfung abgeschlossen")
        st.checkbox("Inhaltliche Prüfung abgeschlossen")

    # Tab Benutzerkontos im Datenkatalog
    with tabs[2]:
        st.subheader("Benutzerkontos im Datenkatalog")
        st.info("Verarbeite die validierten Daten.")
        if st.button("Verarbeitung starten"):
            with st.spinner("Verarbeite..."):
                # Hier folgt deine Logik
                st.success("Verarbeitung abgeschlossen.")

    # Tab Projekt im Datenkatalog
    with tabs[3]:
        st.subheader("Projekt im Datenkatalog")
        st.download_button("Ergebnis herunterladen", data="Ergebnisdaten...", file_name="ergebnis.txt")

    # Tab Datensatz im Datenkatalog
    with tabs[4]:
        st.subheader("Datensatz im Datenkatalog")
        st.success("Workflow abgeschlossen.")


#Mögliche Erweiterungen:
#- Pro Tab eigene Funktionen auslagern, z.B. workflow_step_1(), wenn die Logik umfangreicher wird.
#- Für Zustandsverwaltung über Schritte hinweg st.session_state verwenden.
#- Auf „Zurück“- oder „Weiter“-Buttons wechseln (anstelle von Tabs).
#- Direkt Verzweigungen im Workflow (z.B. je nach Auswahl einen anderen Pfad)

def main():
    st.set_page_config(page_title="Workflow", layout="wide")
    st.title("Workflow-Übersicht")

    # Radio-Buttons für die Workflow-Schritte
    subpages = st.sidebar.radio("Bearbeitungs-Schritt wählen", ["1 DCC: Vorbereitung","2 DS: Erste Prüfung","3 DO: Zweite Prüfung","4 DCC: Abschluss"])

    if subpages == "1 DCC: Vorbereitung":
        st.header("1 DCC: Vorbereitung")
        dcc_vorb()
    elif subpages == "2 DS: Erste Prüfung":
        st.header("2 DS: Erste Prüfung")
    elif subpages == "3 DO: Zweite Prüfung":
        st.header("3 DO: Zweite Prüfung")
    else:
        st.header("4 DCC: Abschluss")

if __name__ == "__main__":
    main()